module HBS2.Sync.Mount
  ( mountPath
  ) where

import HBS2.Sync.Prelude
import HBS2.Sync.State


import HBS2.CLI.Run.MetaData (getTreeContents)
import HBS2.Net.Messaging.Unix
import HBS2.Net.Proto.Service qualified as HBS2
import HBS2.Peer.CLI.Detect (detectRPC)
import HBS2.Peer.RPC.API.Storage qualified as Storage
import HBS2.Peer.RPC.Client qualified as Client
import HBS2.Peer.RPC.Client.StorageClient qualified as Client
import HBS2.Peer.RPC.Client.Unix
import HBS2.Peer.RPC.Client.Unix (UNIX)

import Control.Monad.Except (runExceptT)
import Data.ByteString.Char8 qualified as BS
import Data.ByteString.Lazy qualified as LBS
import Data.List qualified as List
import Data.Map qualified as Map
import Data.Time.Format qualified as Time
import Data.Time.Clock qualified as Time
import Lens.Micro.Platform
import System.Fuse (FuseOperations(..))
import System.Fuse qualified as Fuse
import System.IO qualified as IO
import System.Posix.Files qualified as Posix
import System.Posix.Types qualified as Posix

type FuseOp a = IO (Either Fuse.Errno a)

type Tree = Map.Map FilePath Entry

data State = State
  { storage :: AnyStorage
  , tree :: Tree
  }

rootPath :: FilePath
rootPath = "/"

buildTree :: Foldable t => t Entry -> Map.Map FilePath Entry
buildTree entries =
  let
    addDirs entry =
      if isFile entry then
        entriesFromFile (getEntryHash entry) (getEntryTimestamp entry) (entryPath entry)
      else
        Map.empty
  in
  entries
    & foldl (\acc entry -> Map.insert (entryPath entry) entry acc) Map.empty
    & foldr (\entry acc -> Map.union (addDirs entry) acc)  Map.empty

dirStat :: Fuse.FuseContext -> Fuse.FileStat
dirStat ctx =
  let
    statEntryType = Fuse.Directory
    statFileMode =
      foldr1 Posix.unionFileModes
        [ Posix.ownerReadMode
        , Posix.ownerExecuteMode
        , Posix.groupReadMode
        , Posix.groupExecuteMode
        , Posix.otherReadMode
        , Posix.otherExecuteMode
        ]
    statLinkCount = 2
    statFileOwner = Fuse.fuseCtxUserID ctx
    statFileGroup = Fuse.fuseCtxGroupID ctx
    statSpecialDeviceID = 0
    statFileSize = 4096
    statBlocks = 1
    statAccessTime = 0
    statModificationTime = 0
    statStatusChangeTime = 0
  in
  Fuse.FileStat { .. }

fileStat :: Fuse.FuseContext -> Fuse.FileStat
fileStat ctx =
  let
    statEntryType = Fuse.RegularFile
    statFileMode =
      foldr1 Posix.unionFileModes
        [ Posix.ownerReadMode
        , Posix.groupReadMode
        , Posix.otherReadMode
        ]
    statLinkCount = 1
    statFileOwner = Fuse.fuseCtxUserID ctx
    statFileGroup = Fuse.fuseCtxGroupID ctx
    statSpecialDeviceID = 0
    statFileSize = 4096
    statBlocks = 1
    statAccessTime = 0
    statModificationTime = 0
    statStatusChangeTime = 0
  in
  Fuse.FileStat { .. }

initStorage :: IO (Maybe AnyStorage)
initStorage = do
  maybeSoname <- detectRPC
  case maybeSoname of
    Just soname -> do
      flip runContT pure do
        client <- lift $ race (pause @'Seconds 1) (newMessagingUnix False 1.0 soname)
                  >>= orThrowUser ("can't connect to" <+> pretty soname)

        void $ ContT $ withAsync $ runMessagingUnix client
        storageAPI <- HBS2.makeServiceCaller @Storage.StorageAPI (fromString soname)
        let endpoints = [ Endpoint @UNIX  storageAPI ]
        void $ ContT $ withAsync $ liftIO $ runReaderT (runServiceClientMulti endpoints) client
        return $ Just $ AnyStorage (Client.StorageClient storageAPI)

    _ ->
      pure Nothing

onInit :: IORef (Maybe AnyStorage) -> IO ()
onInit ref = do
  return ()
  --storage <- initStorage
  --writeIORef ref storage

onGetFileStat :: Tree -> FilePath -> FuseOp Fuse.FileStat
onGetFileStat tree path
  | path == rootPath =
    Right . dirStat <$> Fuse.getFuseContext

  | otherwise =
    case Map.lookup (dropWhile (== '/') path) tree of
      Just (DirEntry (EntryDesc { entryType = Dir }) _) ->
        Right . dirStat <$> Fuse.getFuseContext

      Just (DirEntry (EntryDesc { entryType = File }) _) ->
        Right . fileStat <$> Fuse.getFuseContext

      _ ->
        return $ Left Fuse.eNOENT

onOpen :: Tree -> FilePath -> Fuse.OpenMode -> Fuse.OpenFileFlags -> FuseOp ()
onOpen tree path mode _flags =
  case Map.lookup (dropWhile (== '/') path) tree of
    Just (DirEntry (EntryDesc { entryType = File }) _) ->
      case mode of
        Fuse.ReadOnly ->
          return $ Right ()

        _ ->
          return $ Left Fuse.eACCES

    _ ->
      return $ Left Fuse.eNOENT

onRead :: IORef (Maybe AnyStorage) -> Tree -> FilePath -> () -> Posix.ByteCount -> Posix.FileOffset -> FuseOp BS.ByteString
onRead ref tree path _ byteCount offset = do
  case Map.lookup (dropWhile (== '/') path) tree of
    Just entry@(DirEntry (EntryDesc { entryType = File }) _) ->
      case getEntryHash entry of
        Just hash -> do
          maybeSoname <- detectRPC
          case maybeSoname of
            Just soname -> do
              flip runContT pure do
                client <- lift $ race (pause @'Seconds 1) (newMessagingUnix False 1.0 soname)
                          >>= orThrowUser ("can't connect to" <+> pretty soname)

                void $ ContT $ withAsync $ runMessagingUnix client
                storageAPI <- HBS2.makeServiceCaller @Storage.StorageAPI (fromString soname)
                let endpoints = [ Endpoint @UNIX  storageAPI ]
                void $ ContT $ withAsync $ liftIO $ runReaderT (runServiceClientMulti endpoints) client
                let storage = AnyStorage (Client.StorageClient storageAPI)
                eitherContent <- lift $ runExceptT (getTreeContents storage hash)
                case eitherContent of
                  Right content ->
                    content
                      & LBS.drop (fromIntegral offset)
                      & LBS.take (fromIntegral byteCount)
                      & LBS.toStrict
                      & Right
                      & return
                  _ ->
                    return $ Left Fuse.eNOENT
            _ ->
              return $ Left Fuse.eNOENT
        _ ->
          return $ Left Fuse.eNOENT
    _ ->
      return $ Left Fuse.eNOENT


onOpenDirectory :: Monad m => Tree -> String -> m Fuse.Errno
onOpenDirectory tree path
  | path == rootPath =
    return Fuse.eOK

  | otherwise =
    case Map.lookup (dropWhile (== '/') path) tree of
      Just (DirEntry (EntryDesc { entryType = Dir }) _) ->
        return Fuse.eOK

      _ ->
        return Fuse.eNOENT

stat context tree prefix path =
  case Map.lookup (prefix <> path) tree of
    Just (DirEntry (EntryDesc { entryType = Dir }) _) ->
      [(path, dirStat context)]

    Just (DirEntry (EntryDesc { entryType = File }) _) ->
      [(path, fileStat context)]

    _ ->
      []

onReadDirectory :: Tree -> FilePath -> FuseOp [(FilePath, Fuse.FileStat)]
onReadDirectory tree path
  | path == rootPath = do
    context <- Fuse.getFuseContext
    let entries =
          Map.keys tree
            & map (takeWhile (/= '/'))
            & List.nub
            & concatMap (stat context tree "")

    return $ Right $
      [ (".", dirStat context)
      , ("..", dirStat context)
      ] <> entries

  | otherwise =
    let
      prefix = (dropWhile (== '/') path) <> "/"
    in
    case Map.lookup (dropWhile (== '/') path) tree of
      Just (DirEntry (EntryDesc { entryType = Dir }) _) -> do
        context <- Fuse.getFuseContext
        let entries =
              Map.keys tree
                & filter (List.isPrefixOf prefix)
                & map (\path -> takeWhile (/= '/') $ fromMaybe path $ List.stripPrefix prefix path)
                & List.nub
                & concatMap (stat context tree prefix)

        return $ Right $
          [ (".", dirStat context)
          , ("..", dirStat context)
          ] <> entries

      _ ->
        return $ Left Fuse.eNOENT

onGetFileSystemStats :: String -> FuseOp Fuse.FileSystemStats
onGetFileSystemStats _ =
  return $ Right $ Fuse.FileSystemStats
    { fsStatBlockSize = 512
    , fsStatBlockCount = 1
    , fsStatBlocksFree = 1
    , fsStatBlocksAvailable = 1
    , fsStatFileCount = 5
    , fsStatFilesFree = 10
    , fsStatMaxNameLength = 255
    }

operations :: IORef (Maybe AnyStorage) -> Tree -> Fuse.FuseOperations ()
operations ref tree =
  Fuse.defaultFuseOps
    { fuseGetFileStat = onGetFileStat tree
    , fuseGetFileSystemStats = onGetFileSystemStats
    , fuseInit = onInit ref
    , fuseOpen = onOpen tree
    , fuseOpenDirectory = onOpenDirectory tree
    , fuseRead = onRead ref tree
    , fuseReadDirectory = onReadDirectory tree
    }

mountPath ::
  forall c m.
  ( IsContext c
  , MonadUnliftIO m
  )
  => [Entry]
  -> FilePath
  -> RunM c m ()
mountPath entries path = do
  let tree = buildTree entries
  ref <- newIORef Nothing
  liftIO $ Fuse.fuseRun "sync mount" [path] (operations ref tree) Fuse.defaultExceptionHandler

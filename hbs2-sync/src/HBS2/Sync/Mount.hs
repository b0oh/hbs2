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
import Data.STRef (newSTRef, writeSTRef)

type FuseOp a = IO (Either Fuse.Errno a)

--type Tree = HM.HashMap FilePath Entry

--data Tree = Dir FilePath [Tree] | File FilePath Entry

type Tree = Map.Map FilePath Entry


openLog :: FilePath -> IO Handle
openLog path = do
  handle <- openFile path ReadWriteMode
  IO.hSeek handle SeekFromEnd 0
  return handle

_log :: Handle -> String -> IO ()
_log handle line = do
  time <- Time.getCurrentTime
  let prefix = Time.formatTime Time.defaultTimeLocale "%F %T%Q" time
  IO.hPutStr handle $ "[" <> prefix <> "] "
  IO.hPutStrLn handle line
  IO.hFlush handle

rootPath :: FilePath
rootPath = "/"


opInit :: IORef AnyStorage -> IO ()
opInit storageRef = do
  maybeSoname <- detectRPC
  case maybeSoname of
    Just soname -> do
      storageAPI :: Client.ServiceCaller Storage.StorageAPI UNIX <- HBS2.makeServiceCaller @Storage.StorageAPI (fromString soname)
      let storage = AnyStorage (Client.StorageClient storageAPI)
      writeIORef storageRef storage
    Nothing ->
      pure ()

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

getFileStat :: Tree -> FilePath -> FuseOp Fuse.FileStat
getFileStat tree path
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

opOpen :: Tree -> FilePath -> Fuse.OpenMode -> Fuse.OpenFileFlags -> FuseOp ()
opOpen tree path mode _flags =
  case Map.lookup (dropWhile (== '/') path) tree of
    Just (DirEntry (EntryDesc { entryType = File }) _) ->
      case mode of
        Fuse.ReadOnly ->
          return $ Right ()

        _ ->
          return $ Left Fuse.eACCES

    Nothing ->
      return $ Left Fuse.eNOENT

opRead :: IORef AnyStorage -> Tree -> FilePath -> () -> Posix.ByteCount -> Posix.FileOffset -> FuseOp BS.ByteString
opRead storageRef tree path _ byteCount offset = do
  storage <- readIORef storageRef
  wtf <- openLog "/Users/dima/reads.log"
  _log wtf "one"

  maybeSoname <- detectRPC
  case maybeSoname of
    Just soname -> do

      _log wtf soname
      storageAPI :: Client.ServiceCaller Storage.StorageAPI UNIX <- HBS2.makeServiceCaller @Storage.StorageAPI (fromString soname)
      let storage = AnyStorage (Client.StorageClient storageAPI)

      case Map.lookup (dropWhile (== '/') path) tree of
        Just entry@(DirEntry (EntryDesc { entryType = File }) _) -> do
          _log wtf "two"
          case getEntryHash entry of
            Just hash -> do
              _log wtf "tree"
              let tc = getTreeContents storage hash
              _log wtf "four"
              eitherContent <- runExceptT tc
              _log wtf "five"
              case eitherContent of
                Right content -> do
                  _log wtf "six"

                  content
                    & LBS.drop (fromIntegral offset)
                    & LBS.take (fromIntegral byteCount)
                    & LBS.toStrict
                    & Right
                    & return

                Left _ ->
                  return $ Left Fuse.eNOENT

            Nothing ->
              return $ Left Fuse.eNOENT

        _ ->
          return $ Left Fuse.eNOENT

openDirectory :: Monad m => Tree -> String -> m Fuse.Errno
openDirectory tree path
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

readDirectory :: Tree -> FilePath -> FuseOp [(FilePath, Fuse.FileStat)]
readDirectory tree path
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

getFileSystemStats :: String -> FuseOp Fuse.FileSystemStats
getFileSystemStats _ =
  return $ Right $ Fuse.FileSystemStats
    { fsStatBlockSize = 512
    , fsStatBlockCount = 1
    , fsStatBlocksFree = 1
    , fsStatBlocksAvailable = 1
    , fsStatFileCount = 5
    , fsStatFilesFree = 10
    , fsStatMaxNameLength = 255
    }

operations :: IORef AnyStorage -> Tree -> Fuse.FuseOperations ()
operations ref tree =
  Fuse.defaultFuseOps
    { fuseGetFileStat = getFileStat tree
    , fuseGetFileSystemStats = getFileSystemStats
    , fuseInit = opInit ref
    , fuseOpen = opOpen tree
    , fuseOpenDirectory = openDirectory tree
    , fuseRead = opRead ref tree
    , fuseReadDirectory = readDirectory tree
    }

mountPath ::
  forall c m.
  ( Client.HasClientAPI Storage.StorageAPI UNIX m
  , HasStorage m
  , IsContext c
  , MonadUnliftIO m
  )
  => [Entry]
  -> FilePath
  -> RunM c m ()
mountPath entries path = do
  storage <- getStorage
  storageRef <- newIORef storage
  let tree = buildTree entries

  liftIO $ putStrLn $ show tree

  case Map.lookup "yo" tree of
    Just entry@(DirEntry (EntryDesc { entryType = File }) _) ->
      case getEntryHash entry of
        Just hash -> do
          eitherContent <- runExceptT (getTreeContents storage hash)
          case eitherContent of
            Right content ->
             liftIO $ putStrLn $ show content

  liftIO $ Fuse.fuseRun "sync mount" [path] (operations storageRef tree) Fuse.defaultExceptionHandler

module HBS2.Sync.Mount
  ( mountPath
  ) where

import HBS2.Sync.Prelude
import HBS2.Sync.State

import HBS2.Peer.RPC.API.Peer qualified as Peer
import HBS2.Peer.RPC.API.RefChan qualified as RefChan
import HBS2.Peer.RPC.API.Storage qualified as Storage
import HBS2.Peer.RPC.Client qualified as Client
import HBS2.Peer.RPC.Client.Unix (UNIX)

import Data.ByteString.Char8 qualified as BS
import Data.List qualified as List
import Data.Map qualified as Map
import Lens.Micro.Platform
import System.Fuse (FuseOperations(..))
import System.Fuse qualified as Fuse
import System.Posix.Files qualified as Posix
import System.Posix.Types qualified as Posix

type FuseOp a = IO (Either Fuse.Errno a)

--type Tree = HM.HashMap FilePath Entry

--data Tree = Dir FilePath [Tree] | File FilePath Entry

type Tree = Map.Map FilePath Entry

helloString :: BS.ByteString
helloString = BS.pack "Hello Fuse!\n"

rootPath :: FilePath
rootPath = "/"

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

opRead :: Tree -> FilePath -> () -> Posix.ByteCount -> Posix.FileOffset -> FuseOp BS.ByteString
opRead tree path _ byteCount offset =
  case Map.lookup (dropWhile (== '/') path) tree of
    Just (DirEntry (EntryDesc { entryType = File }) _) ->
      helloString
        & BS.drop (fromIntegral offset)
        & BS.take (fromIntegral byteCount)
        & Right
        & return

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

operations :: Tree -> Fuse.FuseOperations ()
operations tree =
  Fuse.defaultFuseOps
    { fuseGetFileStat = getFileStat tree
    , fuseOpen = opOpen tree
    , fuseRead = opRead tree
    , fuseOpenDirectory = openDirectory tree
    , fuseReadDirectory = readDirectory tree
    , fuseGetFileSystemStats = getFileSystemStats
    }

mountPath ::
  forall c m.
  ( Client.HasClientAPI Peer.PeerAPI UNIX m
  , Client.HasClientAPI RefChan.RefChanAPI UNIX m
  , Client.HasClientAPI Storage.StorageAPI UNIX m
  , HasKeyManClient m
  , IsContext c
  , MonadUnliftIO m
  )
  => [Entry]
  -> FilePath
  -> RunM c m ()
mountPath entries path = do
  let tree = buildTree entries

  liftIO $ putStrLn $ show tree
  liftIO $ Fuse.fuseRun "sync mount" [path] (operations tree) Fuse.defaultExceptionHandler

// asset 디렉토리 접근 계층 — File System Access API 로 디렉토리를 선택/복원하고,
// 파일 핸들을 스캔해 basename 으로 파일 텍스트를 읽어준다. 파일 포맷은 모른다.
// 디렉토리 핸들은 IndexedDB 에 저장해 새로고침 후에도 (권한 재확인만으로) 복원한다.

/* ------------------------------------------------------------------ */
/* 디렉토리 선택 / 스캔                                                 */
/* ------------------------------------------------------------------ */

const scanDirectoryRecursively = async (directoryHandle, relativePath = '') => {
  const fileHandles = [];

  for await (const [name, handle] of directoryHandle.entries()) {
    const fullPath = relativePath ? `${relativePath}/${name}` : name;

    if (handle.kind === 'file' && name.toLowerCase().endsWith('.json')) {
      handle.relativePath = fullPath;
      fileHandles.push(handle);
    } else if (handle.kind === 'directory') {
      const subFiles = await scanDirectoryRecursively(handle, fullPath);
      fileHandles.push(...subFiles);
    }
  }

  return fileHandles;
};

const toFileInfo = (handle) => ({
  name: handle.name,
  path: handle.relativePath || handle.name,
});

export const selectDirectoryAndGetFiles = async () => {
  const directoryHandle = await window.showDirectoryPicker();
  await storeDirectoryHandle(directoryHandle);
  const fileHandles = await scanDirectoryRecursively(directoryHandle);
  storeFileHandles(fileHandles);
  return fileHandles.map(toFileInfo);
};

export const restoreFilesFromDirectory = async () => {
  const directoryHandle = await getStoredDirectoryHandle();
  if (!directoryHandle) {
    throw new Error('No directory handle found. Please select directory first.');
  }

  const permission = await directoryHandle.queryPermission({mode: 'read'});
  if (permission !== 'granted') {
    const requestPermission = await directoryHandle.requestPermission({mode: 'read'});
    if (requestPermission !== 'granted') {
      throw new Error('Permission denied to read directory');
    }
  }

  const fileHandles = await scanDirectoryRecursively(directoryHandle);
  storeFileHandles(fileHandles);
  return fileHandles.map(toFileInfo);
};

/* ------------------------------------------------------------------ */
/* 파일 읽기 (basename 매칭)                                            */
/* ------------------------------------------------------------------ */

export const fetchFileText = async (fileName) => {
  const fileHandles = getStoredFileHandles();
  if (!fileHandles || fileHandles.length === 0) {
    throw new Error('No file handles found. Please select directory first.');
  }
  const fileHandle = fileHandles.find(handle => handle.name === fileName);
  if (!fileHandle) {
    throw new Error(`File not found: ${fileName}`);
  }
  const file = await fileHandle.getFile();
  return file.text();
};

/* ------------------------------------------------------------------ */
/* 파일 삭제 (FileSystemFileHandle.remove(), Chrome 108+ — 이 앱은 이미     */
/* showDirectoryPicker() 에 의존하므로 Chromium 전용 제약이 새로 생기지 않음)  */
/* ------------------------------------------------------------------ */

export const deleteFiles = async (fileNames) => {
  const directoryHandle = await getStoredDirectoryHandle();
  if (!directoryHandle) {
    throw new Error('No directory handle found. Please select directory first.');
  }

  const permission = await directoryHandle.queryPermission({mode: 'readwrite'});
  if (permission !== 'granted') {
    const requested = await directoryHandle.requestPermission({mode: 'readwrite'});
    if (requested !== 'granted') {
      throw new Error('Permission denied to delete files (readwrite)');
    }
  }

  const fileHandles = getStoredFileHandles();
  const deleted = [];
  const failed = [];

  for (const name of fileNames) {
    const handle = fileHandles.find(h => h.name === name);
    if (!handle) {
      failed.push({name, error: new Error(`File not found in cache: ${name}`)});
      continue;
    }
    try {
      await handle.remove();
      deleted.push(name);
    } catch (error) {
      if (error?.name === 'NotFoundError') {
        deleted.push(name); // 이미 지워짐 — 재시도에 대해 idempotent
      } else {
        console.error(`Error deleting file ${name}:`, error);
        failed.push({name, error});
      }
    }
  }

  if (deleted.length > 0) {
    const deletedSet = new Set(deleted);
    storeFileHandles(fileHandles.filter(h => !deletedSet.has(h.name)));
  }

  return {deleted, failed};
};

/* ------------------------------------------------------------------ */
/* 파일 쓰기 (FileSystemFileHandle.createWritable(), 전체 내용 덮어쓰기)      */
/* ------------------------------------------------------------------ */

export const writeFileText = async (fileName, text) => {
  const directoryHandle = await getStoredDirectoryHandle();
  if (!directoryHandle) {
    throw new Error('No directory handle found. Please select directory first.');
  }

  const permission = await directoryHandle.queryPermission({mode: 'readwrite'});
  if (permission !== 'granted') {
    const requested = await directoryHandle.requestPermission({mode: 'readwrite'});
    if (requested !== 'granted') {
      throw new Error('Permission denied to write file (readwrite)');
    }
  }

  const fileHandles = getStoredFileHandles();
  const fileHandle = fileHandles.find(h => h.name === fileName);
  if (!fileHandle) {
    throw new Error(`File not found in cache: ${fileName}`);
  }

  const writable = await fileHandle.createWritable();
  await writable.write(text);
  await writable.close();
};

/* ------------------------------------------------------------------ */
/* IndexedDB / 메모리 핸들 저장                                          */
/* ------------------------------------------------------------------ */

const storeDirectoryHandle = async (directoryHandle) => {
  try {
    window._directoryHandle = directoryHandle;
    const db = await openIndexedDB();
    const transaction = db.transaction(['fileHandles'], 'readwrite');
    const store = transaction.objectStore('fileHandles');
    await store.put(directoryHandle, 'directoryHandle');
    localStorage.setItem('csvDirectorySelected', 'true');
  } catch (error) {
    console.error('Error storing directory handle:', error);
    window._directoryHandle = directoryHandle;
    localStorage.setItem('csvDirectorySelected', 'true');
  }
};

const getStoredDirectoryHandle = async () => {
  if (window._directoryHandle) {
    return window._directoryHandle;
  }
  try {
    const db = await openIndexedDB();
    const transaction = db.transaction(['fileHandles'], 'readonly');
    const store = transaction.objectStore('fileHandles');
    return new Promise((resolve) => {
      const request = store.get('directoryHandle');
      request.onsuccess = () => {
        const handle = request.result;
        if (handle) {
          window._directoryHandle = handle;
          resolve(handle);
        } else {
          resolve(null);
        }
      };
      request.onerror = () => {
        console.error('Error retrieving directory handle from IndexedDB');
        resolve(null);
      };
    });
  } catch (error) {
    console.error('Error accessing IndexedDB:', error);
    return null;
  }
};

const openIndexedDB = () => {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open('StreamedTraderDB', 1);
    request.onerror = () => reject(new Error('Failed to open IndexedDB'));
    request.onsuccess = () => resolve(request.result);
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains('fileHandles')) {
        db.createObjectStore('fileHandles');
      }
    };
  });
};

const storeFileHandles = (fileHandles) => {
  window._fileHandles = fileHandles;
};

const getStoredFileHandles = () => {
  return window._fileHandles || [];
};

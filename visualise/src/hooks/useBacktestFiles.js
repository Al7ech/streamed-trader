import {useCallback, useEffect, useState} from 'react';
import {restoreFilesFromDirectory, selectDirectoryAndGetFiles} from '../utils/fileAccess';
import {deleteRun as deleteRunFiles, filterRunFiles} from '../utils/runLoader';

/**
 * asset 디렉토리 선택/복원과 run 파일 목록(backtest/*.json, series 제외)을 관리하는 훅.
 * 라우트(RunListPage/RunDetailPage)와 무관하게 앱 루트에서 한 번만 호출한다.
 */
export const useBacktestFiles = () => {
  const [loading, setLoading] = useState(true);
  const [directorySelected, setDirectorySelected] = useState(false);
  const [backtestFiles, setBacktestFiles] = useState([]);

  useEffect(() => {
    const checkDirectoryStatus = async () => {
      const wasSelected = localStorage.getItem('csvDirectorySelected');
      if (wasSelected) {
        try {
          const restoredFiles = await restoreFilesFromDirectory();
          setBacktestFiles(filterRunFiles(restoredFiles));
          setDirectorySelected(true);
        } catch (error) {
          console.error('Error restoring files:', error);
          localStorage.removeItem('csvDirectorySelected');
          setDirectorySelected(false);
        }
      }
      setLoading(false);
    };
    checkDirectoryStatus();
  }, []);

  const selectDirectory = useCallback(async () => {
    try {
      setLoading(true);
      const files = await selectDirectoryAndGetFiles();
      setBacktestFiles(filterRunFiles(files));
      setDirectorySelected(true);
    } catch (error) {
      console.error('Error selecting directory:', error);
      alert('디렉토리 선택 중 오류가 발생했습니다. 다시 시도해주세요.');
    } finally {
      setLoading(false);
    }
  }, []);

  const removeRun = useCallback(async (fileName) => {
    const {deleted, failed} = await deleteRunFiles(fileName);
    if (deleted.includes(fileName)) {
      setBacktestFiles(prev => prev.filter(f => f.name !== fileName));
    }
    if (failed.length > 0) {
      throw new Error(`Failed to delete ${failed.length} file(s): ${failed.map(f => f.name).join(', ')}`);
    }
  }, []);

  return {loading, directorySelected, backtestFiles, selectDirectory, removeRun};
};

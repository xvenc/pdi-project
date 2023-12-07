import filecmp
import unittest
import os

class TestFileComparison(unittest.TestCase):

    def test_compare_all(self):
        for i in range(1, 7):
            folder_path = 'logs/task' + str(i) +'/'
            # List all files in the folder
            files = os.listdir(folder_path)
            json_files = [file for file in files if file.endswith('.json') and os.path.isfile(os.path.join(folder_path, file))]
            for json_file in json_files:
                file_path = os.path.join(folder_path, json_file)
                test_file_path = 'test/task' + str(i) + '.json'
                self.assertTrue(filecmp.cmp(file_path, test_file_path), f'Files {file_path} and {test_file_path} are not equal')

if __name__ == '__main__':
    unittest.main() 
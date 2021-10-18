'''
列出某个目录及其子目录下文件名
'''

import os,re
# file_list = []

class FILE_Find():
    def __init__(self,dirname,filename):
        self.filepath = dirname
        self.filename = filename
        self.flag = 0
        self.file_list = []

        pass

    def find_file(self,filepath):
        searchlist = os.listdir(filepath)

        if self.filename in searchlist:  # 基本条件
            print(os.path.join(filepath, self.filename))
            self.flag = 1
            return
        # 遍历当前文件列表
        for file in searchlist:
            subpath = filepath + "\\" + file

            if os.path.isdir(subpath):
                self.find_file(subpath)
                if self.flag == 1:
                    break

    def list_all_file(self,filepath):
        searchlist = os.listdir(filepath)

        # 遍历当前文件列表
        for file in searchlist:
            subpath = filepath + "\\" + file

            if os.path.isdir(subpath):

                if  re.search('[音频|英语]$',subpath):
                    continue
                self.list_all_file(subpath)
                print(subpath)

            else:
                if not os.path.splitext(file)[1] in ['.pdf','.epub','.doc','docx','.txt']:
                    self.file_list.append(subpath)

        pass

    def list_audio_file(self,filepath):
        searchlist = os.listdir(filepath)

        # 遍历当前文件列表
        for file in searchlist:
            subpath = filepath + "\\" + file

            if os.path.isdir(subpath):

                if  re.search('[英语]$',subpath):
                    continue
                self.file_list.append(subpath)
                self.list_audio_file(subpath)
                print(subpath)


        pass


if __name__ == '__main__':

    root_filepath = "E:\我的书库"
    filename = 'Untitled.csv'

    file = FILE_Find(root_filepath,filename)

    # file.find_file(root_filepath)
    # file.list_all_file(root_filepath)
    # with open('book_index.txt','a',encoding='utf-8') as f:
    #         f.writelines([ele+"\n" for ele in file.file_list])

    file.list_audio_file(root_filepath)
    with open('book_index_audio.txt', 'a', encoding='utf-8') as f:
        f.writelines([ele + "\n" for ele in file.file_list])


    print(file.file_list)

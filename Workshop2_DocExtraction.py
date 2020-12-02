import os
import sys
import GDocAI

def main():
    _file_name = ''
    file_name = sys.argv[1] if len(sys.argv) > 1 else _file_name

    GDocAI.configure()
    uploaded_name = GDocAI.upload_cloud(file_name)
    dataFrame = GDocAI.parse_table(uploaded_name)

    for k, v in dataFrame.items():
        print(v)

if __name__ == '__main__':
    main()

import os


class Html(object):

    __root_html = os.path.dirname(os.path.abspath(__name__))

    def __init__(self, name_file: str):
        self.__name_file = name_file

    def get_html(self) -> str:
        dir_html = self.__root_html + f'\\template\\{self.__name_file}.html'
        with open(dir_html, 'r') as file_html:
            html = file_html.read()

        return html

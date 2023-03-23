class TextColors:

    """Collection of static variables and methods to assist in coloring text."""

    # ANSI color codes
    BLUE = '\033[34m'
    BOLD = '\033[1m'
    CYAN = '\033[36m'
    DEFAULT = '\033[0m'
    GREEN = '\033[32m'
    GREY = '\033[38;5;242m'
    MAGENTA = '\033[35m'
    RED = '\033[31m'
    YELLOW = '\033[33m'

    @staticmethod
    def bold(text: str) -> str:
        """Return bolded text.

        :param text: a string
        :return: text color coded with ANSI bold
        """
        return TextColors.color_text(TextColors.BOLD, text)

    @staticmethod
    def blue(text: str) -> str:
        """Return text that can be printed blue.

        :param text: a string
        :return: text color coded with ANSI blue
        """
        return TextColors.color_text(TextColors.BLUE, text)

    @staticmethod
    def green(text: str) -> str:
        """Return text that can be printed green.

        :param text: a string
        :return: text color coded with ANSI green"""
        return TextColors.color_text(TextColors.GREEN, text)

    @staticmethod
    def red(text: str) -> str:
        """Return text that can be printed red.

        :param text: a string
        :return: text color coded with ANSI red"""
        return TextColors.color_text(TextColors.RED, text)

    @staticmethod
    def magenta(text: str) -> str:
        """Return text that can be printed magenta.

        :param text: a string
        :return: text color coded with ANSI magenta"""
        return TextColors.color_text(TextColors.MAGENTA, text)

    @staticmethod
    def color_text(color: str, text: str) -> str:
        """Return text that can be printed color.

        :param color: ANSI color code
        :param text: a string
        :return: a string with ANSI color encoding"""
        # any time text returns to default, we want to insert our color.
        replaced = text.replace(TextColors.DEFAULT, TextColors.DEFAULT + color)
        # then wrap the beginning and end in our color/default.
        return color + replaced + TextColors.DEFAULT

    @staticmethod
    def cyan(text: str) -> str:
        """Return text that can be printed cyan.

        :param text: a string
        :return: text color coded with ANSI cyan"""
        return TextColors.color_text(TextColors.CYAN, text)

    @staticmethod
    def yellow(text: str) -> str:
        """Return text that can be printed yellow.

        :param text: a string
        :return: text color coded with ANSI yellow"""
        return TextColors.color_text(TextColors.YELLOW, text)

    @staticmethod
    def grey(text: str) -> str:
        return TextColors.color_text(TextColors.GREY, text)

    @staticmethod
    def default(text: str) -> str:
        return TextColors.color_text(TextColors.DEFAULT, text)

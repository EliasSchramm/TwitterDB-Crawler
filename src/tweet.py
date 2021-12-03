from typing import List
import emoji
import string


class Tweet:
    def __init__(self, text: str):

        self.char_blacklist = [
            "\n",
            "\t",
            ".",
            ",",
            "(",
            ")",
            "{",
            "}",
            "-",
            "+",
            ":",
            "/",
            "\\",
            "'",
            '"',
            "!",
            "?",
            "=",
            "…",
            "*",
            "&",
            "€",
            "$",
            ";",
            "・",
            "。",
            "．．．",
            "、",
            "⋮",
            " ",
            " ",
            "[",
            "]",
        ]
        self.text: str = self._clean(text, self.char_blacklist)

        self.text = self.text.lower()
        self.hashtags = self._find("#", forbidden="@")
        self._clean_tag()
        self.tags = self._find("@", forbidden="#")

    def _clean(self, text: str, forbidden: List[str]) -> str:
        for x in forbidden:
            text = text.replace(x, " ")
        return text

    def _find(self, prefix, forbidden):
        ret = []
        _text = self.text
        _text = _text.replace(forbidden, " ")
        _text = _text.replace("　", "")
        _text = _text.replace("！", "")

        if not _text.startswith("RT"):

            for word in _text.split(" "):
                word = self._remove_emojis(word)

                if len(word) >= 2 and word.count(prefix) == 1:
                    word = word.split(prefix)
                    word = prefix + word[len(word) - 1]
                    word = word.strip()

                    if word not in ret and len(word) >= 2 and word.startswith(prefix):
                        ret.append(word.lower())

        return ret

    def _remove_emojis(self, s):
        return "".join(c for c in s if c not in emoji.UNICODE_EMOJI["en"])

    def _clean_tag(self):
        allowed = list(
            string.ascii_lowercase + string.ascii_uppercase + string.digits
        ) + ["_", "@", " "]

        newtext = ""
        for letter in self.text:
            if letter in allowed:
                newtext += letter

        self.text = newtext

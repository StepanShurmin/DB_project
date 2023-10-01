import json

from src.abstract_classes import FILESaver


class JSONSaver(FILESaver):
    def __init__(self, data, name):
        self.json_file = f"data_{name}.json"
        self.data = data

    def save_data(self):
        """Записывает данные в файл JSON."""
        with open(self.json_file, "w", encoding="utf-8") as file:
            json.dump(self.data, file, indent=4, ensure_ascii=False)

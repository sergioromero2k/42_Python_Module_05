class FileSaver:
    def save(self, data):
        return "Saving data in generic format"

class JSONSaver(FileSaver):
    def save(self, data):
        return f"Saving as JSON: {data}"

class CSVSaver(FileSaver):
    def save(self, data):
        return f"Saving as CSV: {data}"

class XMLSaver(FileSaver):
    def save(self, data):
        return f"Saving as XML: {data}"


savers = [JSONSaver(), CSVSaver(), XMLSaver()]

for s in savers:
    print(s.save({"a": 1, "b": 2}))

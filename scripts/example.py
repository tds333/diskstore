from diskstore import DiskStore

ds = DiskStore("/tmp/diskstore.db")
ds["key"] = "my value"
print(ds["key"])

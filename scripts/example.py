from diskstore import DiskStore, Value

ds = DiskStore("/tmp/diskstore.db", value_class=Value)
ds["key"] = Value("my value")
print(ds["key"])

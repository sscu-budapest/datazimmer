
def pytest_addoption(parser):
    # test / explore / live
    parser.addoption("--mode", action="store", default="test")

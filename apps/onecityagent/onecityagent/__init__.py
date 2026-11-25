# -*- coding: utf-8 -*-
import sys


if sys.version_info < (3, 11) or sys.version_info > (3, 13):
    print(
        "Warning: Unsupported Python version {ver}, please use 3.11-3.13".format(
            ver=".".join(map(str, sys.version_info))
        )
    )

# 导入并实例化配置
from onecityagent.config import Config
config = Config()

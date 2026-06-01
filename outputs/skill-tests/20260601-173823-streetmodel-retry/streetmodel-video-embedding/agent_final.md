## StreetModel 链路测试结果

| 字段 | 值 |
|------|-----|
| **status** | failed |
| **error_code** | STREETMODEL_REQUEST_FAILED |
| **关键计数** | embedded_count: 0, failed_count: 1 |

**失败原因**：StreetModel 服务端返回 HTTP 400，提示视频文件不存在：`/home/huangxiao/City_brain/imiss-deer-flow-main/datasets/Vedio-demo/Trafic.mp4`

该路径是 DeerFlow 服务器侧的挂载路径，可能文件实际不存在或路径拼写有误（注意 `Vedio-demo` 可能是 `Video-demo` 的拼写错误）。
# 框架来源

原始框架来自本地项目`project-CV-final`，指定提交为`fd16eab6ae91a16b8d14a14fec72f399d72e6c8a`。本目录的`src/`、`pyproject.toml`、`docs/api.md`和`docs/contributor-guide.md`共17份原始文件逐字保留。

原目录保持只读。本项目通过`uv` path dependency使用该目录，业务连接代码在`src/shop/poster.py`。更新来源时需要重新指定提交并核查文件及公开接口。

运行`scripts/verify_poster_source.py --output <项目内证据目录>`，检查来源提交、原始目录、此目录和已安装依赖的逐文件SHA-256。对应验证归属SM-01。

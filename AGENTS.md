# AGENTS

## Proto

- 定义文件：`proto/client.proto`
- 生成文件：`internal/proto/client.pb.go`
- 不要手改生成文件，统一执行：

```bash
./scripts/gen-proto.sh
```

或：

```bash
go generate ./...
```

## 依赖

- 本机需安装 `protoc`
- `protoc-gen-go` 需在 `PATH` 中

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

## 提交约定

- 修改 `proto/client.proto` 后重新生成代码
- 提交 proto 变更时一并提交 `internal/proto/client.pb.go`
- Git 提交信息使用 Conventional Commits 风格

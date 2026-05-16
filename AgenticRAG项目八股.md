# 基于 Go 的 Agentic RAG 知识库问答系统面试八股

## 1. 为什么选择这个项目来做？

我选择这个项目，主要是因为它不是一个单点功能项目，而是一条比较完整的 AI 应用链路。

它从文档上传开始，后面会经过分片上传、MinIO 存储、Kafka 异步建库、Tika 文本抽取、文本切块、Embedding 向量化、Elasticsearch 混合检索，最后再接到 Tool Calling、ReAct、多轮记忆和前端 WebSocket 实时问答。

我觉得这种项目更接近企业里真实的知识库和智能问答场景，因为它不只是“调一下大模型接口”，而是把文档处理、检索、权限、Agent 决策、前端展示这些环节都串起来了。

另外这个项目也比较适合深入讲工程实现。我在这个项目里不仅能讲 RAG，还能讲上传链路、异步解耦、混合检索、工具调用、记忆管理和执行轨迹可视化，所以它既有 AI 应用层的内容，也有比较强的后端工程味。

---

## 2. 这个项目整体是做什么的？

这是一个基于 Go + Vue3 实现的 Agentic RAG 知识库问答系统，主要面向企业内部知识管理场景。

用户可以先上传 PDF、Word、Excel、Markdown 这类文档，系统会自动做解析、切块、向量化和建索引。后面用户提问时，系统会结合 Hybrid Search、Tool Calling 和 ReAct 决策循环，按需调用搜索、摘要、反馈、统计等工具，最后以 WebSocket 的方式把执行过程和最终答案实时返回给前端。

所以它不是一个单纯的“搜索系统”，也不是一个只会调模型的“聊天系统”，而是一个把知识库检索和 Agent 问答结合在一起的系统。

---

## 3. 这个项目的技术栈和核心组件有哪些？

后端主要用的是 Go + Gin + GORM + MySQL + Redis + Kafka + MinIO + Elasticsearch + Apache Tika。

前端主要是 Vue3，然后通过 WebSocket 和后端保持实时通信。

这些组件在项目里的职责大致是：

- MySQL：存用户、文件上传记录、文本分块记录等结构化元数据。
- Redis：存上传分片 bitmap、会话历史、长期记忆、feedback 事件流、工具指标等。
- MinIO：存上传分片对象和合并后的完整文件对象。
- Kafka：把上传和建库解耦，承接异步文件处理任务。
- Apache Tika：从 PDF、Word、Excel 这类文档里抽出纯文本。
- Elasticsearch：存文本块和向量，负责 Hybrid Search。
- WebSocket：把 Agent 的工具状态、执行观察和最终答案实时推送给前端。

---

## 4. 这个项目的整体业务流程是什么？

如果按主链路来讲，我会把它拆成三段：

第一段是文档上传和建库。用户先上传文件，前端按分片上传到后端，后端把分片写入 MinIO、上传状态写入 Redis bitmap、上传元数据写入 MySQL。分片全部上传成功以后，再由后端合并文件，并通过 Kafka 发异步建库任务。

第二段是知识库建库。Kafka Consumer 收到任务以后，会去 MinIO 读取合并后的完整文件，用 Tika 抽取文本，再做固定窗口切块，然后调用 embedding 接口生成向量，最后把文本块和向量一起写入 Elasticsearch。

第三段是用户提问和 Agent 问答。用户通过聊天页提问以后，后端不会固定先检索，而是先进入 Tool Calling + ReAct 决策循环，让模型自己判断要不要调用搜索、摘要、统计等工具。如果调用了搜索工具，就会进入 Hybrid Search，检索结果再作为 tool message 回填给模型，最后生成最终答案并通过 WebSocket 流式返回给前端。

---

## 5. 文档上传的整体流程是怎样的？

这个项目的上传流程大致可以拆成四步。

第一步，前端把文件切成多个分片，然后逐片调用 `/upload/chunk`。

第二步，后端每收到一片，就把这个 chunk 写到 MinIO 的 `chunks/{fileMD5}/{chunkIndex}` 路径下，同时在 MySQL 里记录 chunk 信息，再在 Redis bitmap 里标记这个分片已经上传完成。

第三步，当前端确认所有 chunk 都上传完以后，再调用 `/upload/merge`。后端会把这些分片在 MinIO 里合并成完整文件，目标对象路径是 `merged/{fileName}`，然后把 `file_upload` 主记录状态更新为已完成。

第四步，后端不会在合并请求里同步做建库，而是构造一个 `FileProcessingTask` 发到 Kafka。这样上传请求本身很快就能返回，重任务留给异步消费者处理。

---

## 6. 这个项目上传分片大小设置多少？为什么这么设？

如果面试官问的是“文件上传时的分片大小”，我这个项目的答案是 **5MB**。

前端常量里用的是 `5 * 1024 * 1024`，后端用于计算总分片数的 `DefaultChunkSize` 也是 `5 * 1024 * 1024`，也就是前后端统一按 5MB 分片。

我觉得这个值本质上是一个工程折中。

如果设成 1MB，每个文件会切出更多 chunk，请求次数会明显上升，MinIO 对象数、Redis bitmap 标记次数、MySQL chunk 记录数都会增加，整条上传链的控制开销会变大。

如果设成 10MB，单个分片太大，一旦网络抖动导致失败，重传成本就会更高，而且浏览器和后端一次处理的负载也更重。

所以 5MB 更像是在请求次数、失败重传成本和对象存储处理开销之间做了一个折中，而且这个值在当前项目里也和原有 Java 版本保持一致。

---

## 7. 为什么要做分片上传和断点续传？

主要有三个原因。

第一，大文件上传更稳定。文件拆成多个 chunk 以后，即使某一片失败，也只需要重传失败的那一片，而不是整个文件重传。

第二，更适合弱网环境。如果用户网络不稳定，断点续传可以大大降低上传失败的体感。

第三，工程上更容易做状态管理。当前项目里是用 Redis bitmap 记录哪些 chunk 已经上传成功，这样前端断线重连或者用户重新发起上传时，可以很容易知道哪些分片可以跳过。

---

## 8. 这个项目怎么做秒传和断点续传？

秒传和断点续传主要依赖两层信息。

第一层是 MySQL 里的 `file_upload` 主记录。后端会根据 `fileMD5 + userID` 判断这个文件有没有上传记录，以及当前状态是不是已经完成。

第二层是 Redis bitmap。只要文件记录存在但状态还没完成，后端就会根据总 chunk 数去 Redis 里读取哪些分片已经上传成功，然后把已上传分片序号返回给前端。

这样前端就能知道：

- 如果文件已经完整上传过，就可以直接秒传成功；
- 如果文件上传了一半，就只继续补后面的 chunk。

所以这个项目里的秒传不是简单只靠 MD5 做判断，而是结合了 MySQL 的主记录状态和 Redis bitmap 的分片上传状态。

---

## 9. 为什么文件上传成功不等于知识库可检索？

这是这个项目里我觉得很重要的一个点。

上传成功，只代表上传链路完成了，也就是：

- 分片都传完了；
- MinIO 合并完成了；
- `file_upload` 记录状态更新了。

但这还不代表这个文件已经可检索。后面还要经过：

- Kafka 异步消费；
- Tika 文本抽取；
- 文本切块；
- embedding 向量化；
- ES 写入索引。

只要后面任意一步失败，就会出现“文件在文档列表里可见，但搜索搜不到内容”的情况。

所以在这个项目里，我会明确区分：

- 上传成功
- 建库成功
- 可检索成功

这三个状态不是一回事。

---

## 10. 为什么这里要用 Kafka？

因为上传完成后还有一条很长的重处理链路：

- Tika 抽文本
- 文本切块
- Embedding
- ES 建索引

这些步骤都比较耗时。如果把它们同步放在上传接口里做，前端用户会等很久，而且任何一个步骤失败都会把上传接口拖垮。

所以我这里用了 Kafka 做异步解耦。上传主流程只负责把文件完整落下来，并发出一个异步建库任务；真正的文本抽取、向量化和索引写入，由后台 Consumer 慢慢做。

这样做的好处是：

- 上传接口返回更快；
- 上传链和建库链解耦；
- 后续更容易做重试和补建；
- 整个系统吞吐量更好。

---

## 11. 文本建库流程具体是怎样的？

Kafka Consumer 收到 `FileProcessingTask` 以后，会进入 `Processor.Process(...)` 这条主链。

先从 MinIO 读取已经合并好的完整文件，然后调用 Tika 把原始文件抽成纯文本；接着对文本做固定窗口切块；切块结果会先写入 MySQL 的 `document_vectors` 表，作为中间态持久化；之后再逐块调用 embedding 接口生成 2048 维向量；最后把文本块、向量和权限元数据一起写入 Elasticsearch。

我觉得这里比较有价值的点是，它不是直接“抽文本 -> 立刻塞 ES”，而是先把 chunk 文本落成一个中间态，这样后面即使 embedding 或索引写入失败，系统里也保留了一份可追踪的分块记录，方便排查和补建。

---

## 12. 这个项目的文本切块策略是什么？

当前项目用的是比较基础的固定窗口切块策略。

代码里是：

- `chunkSize = 1000`
- `chunkOverlap = 100`

更准确地说，它更接近 **按字符长度切**，而不是严格按 token 数切。

这套策略的优点是实现简单、稳定，先把整条建库链跑通；但它也有明显的缺点，比如容易打断语义边界，把标题和正文拆开，或者把一个完整段落切成两半。

所以我会把它理解成一个工程上先跑通的第一版方案，而不是最优的 chunking 策略。

---

## 13. 固定窗口切块会有什么问题？

主要有四个问题。

第一，容易截断自然语义边界。比如一句话的前半句在一个 chunk，后半句在另一个 chunk，这样检索回来时上下文就不完整。

第二，标题和正文可能被拆开，影响引用内容的可读性和模型理解。

第三，表格、列表、代码块这种结构化内容更容易被打散。

第四，overlap 只能缓解，不能彻底解决问题。当前 100 字符 overlap 能减少边界截断，但不能真正做到按段落、按标题、按语义单元切块。

所以如果后面要继续优化，我会优先考虑：

- 先按段落切；
- 段落过长再按句子切；
- 尽量保留标题和正文的结构关联。

---

## 14. 为什么这个项目要做 Hybrid Search，而不是只做纯向量检索？

因为纯向量检索虽然语义能力强，但在中文知识库场景下很容易出现两个问题：

第一，对专有名词、术语、缩写和精确表达不够稳；

第二，容易出现“语义上相关，但实际跑题”的结果。

所以这个项目没有只依赖 embedding，而是把向量检索和关键词检索结合起来做 Hybrid Search。

前面用 KNN 做语义召回，后面再通过 `match`、`match_phrase` 和 `rescore` 把文本相关性拉回来，这样既保留语义召回能力，也增强了精确匹配能力。

---

## 15. 为什么也不能只做纯关键词检索？

因为纯关键词检索在语义表达变化比较大的情况下，召回能力不够。

比如用户问的问题和知识库里原文不是同样的措辞，但是语义其实一致。如果只做关键词匹配，就很容易漏召回。

所以这个项目选择 Hybrid Search，本质上是想同时解决两类问题：

- 纯向量检索容易发散；
- 纯关键词检索容易漏掉语义相近表达。

---

## 16. 这个项目的 Hybrid Search 检索链路是什么？

用户提问后，如果走的是独立搜索接口，就会直接调用 `SearchService.HybridSearch(...)`；如果走的是聊天链，就会在 Tool Calling 阶段按需调用搜索工具，最后也是进入这个方法。

这条检索链大致是：

先规范化 `topK`，再计算当前用户的有效组织标签；然后对 query 做 normalize，读取最近反馈生成 `feedbackProfile`；接着调用 embedding 接口生成 query vector；再组装 ES 查询，里面同时包括：

- KNN 向量召回
- must match
- phrase should
- 权限 filter
- rescore

最后解析 ES 结果，再回 MySQL 批量补文件名，返回给上层工具或接口。

---

## 17. 这个项目的 topK 合理吗？

我觉得这个项目的最终 `topK` 是合理的，但偏保守。

当前代码里默认 `topK=5`，最大限制 `10`。这个范围的好处是能平衡证据充分性和噪声控制：

- 如果太小，模型可能拿不到足够证据；
- 如果太大，噪声 chunk 会变多，prompt 也更长，最终回答更容易跑偏。

至于向量召回阶段，项目里用了 `recallK = topK * 30`，也就是默认会先拉 150 个候选，再靠关键词匹配和 rescore 精排回前 5 个结果。

这种设计符合 Hybrid Search 的“先宽召回，再精排序”思路，但这个 30 倍的 recall 倍率比较保守，后续还是值得结合评测继续调优。

---

## 18. 这个项目是怎么做权限过滤的？

权限过滤是在检索层前置做的，不是查完结果以后再在业务层裁。

项目里会先调用 `GetUserEffectiveOrgTags(...)` 计算当前用户的有效组织标签，包括他自己的标签和上级组织继承标签。然后在 ES 查询里直接加 filter，允许三类文档进入结果集：

- 当前用户自己的文档
- 公共文档
- 组织标签命中的文档

我觉得这个设计比较重要，因为如果先查全库再过滤：

- 会浪费召回资源；
- 结果数量不稳定；
- 还有潜在数据泄露风险。

所以这个项目把权限过滤直接写进了 ES 查询条件。

---

## 19. feedback 在这个项目里起什么作用？

feedback 在这个项目里不是单纯的日志，也不只是一个“点赞点踩”记录。

它有两条作用链：

第一条是 prompt 侧。最近的 feedback 事件会被整理成一段 `feedback guidance`，作为 system message 注入到当前轮模型上下文里，影响回答风格和回答策略。

第二条是 retrieval 侧。SearchService 会读取同一批 feedback 事件，生成一个 `searchFeedbackProfile`，动态调整：

- `EffectiveTopK`
- `PhraseBoost`
- `QueryWeight`
- `RescoreWeight`
- `StrictRelevance`

所以它会同时影响回答和检索。

---

## 20. StrictRelevance 在这个项目里影响什么？

`StrictRelevance` 本质上是一个“把检索条件收紧”的反馈开关。

当最近反馈里出现比较明显的“跑题、不相关、不准确”信号时，系统会把 `StrictRelevance` 打开。打开以后，它会让：

- `must match` 从普通 `match` 变成更严格的 `operator=and`
- `rescore` 里的文本匹配也变严
- 同时提高 `PhraseBoost` 和 `RescoreWeight`

整体效果就是：

少相信过于发散的语义召回，多依赖更严格的文本相关性，减少“沾边但不准”的结果。

---

## 21. 这个项目有没有用到 IK 分词器？

有，而且是明确用了。

在 Elasticsearch 索引 mapping 里，`text_content` 字段配置的是：

- 建索引时用 `ik_max_word`
- 搜索时用 `ik_smart`

它主要影响的是 Hybrid Search 里的文本检索通道，也就是：

- `match`
- `match_phrase`
- `rescore`

至于向量检索那条 KNN 通道，和 IK 没关系，它走的是 embedding + 向量相似度。

---

## 22. 这个项目有没有用到 HNSW？

有，但要讲准确一点。

这个项目在 ES 里把 `vector` 字段定义成了 `dense_vector`，并开启了 `index: true`，搜索时也用了 `knn` 查询，所以底层走的是 Elasticsearch / Lucene 的近似近邻检索实现，也就是 HNSW。

但是项目里没有手动显式配置 HNSW 的 `m`、`ef_construction` 这类参数，而是依赖 ES 8.10.4 的默认向量索引行为。

所以更准确的说法是：

**项目用了 ES 默认的 HNSW 能力，但没有手动调 HNSW 参数。**

---

## 23. 这个项目有没有用到独立的 rerank 模型？

没有。

这个项目有“重排序阶段”，但没有接入独立的 rerank model。

当前实现是在 ES 查询内部做 `rescore`，本质上还是基于 `match` / 关键词相关性的一次二次重排序，而不是额外调用一个 cross-encoder 或专门 reranker 模型来给候选结果重新打分。

所以我会说：

- 有 rescore
- 没有独立 rerank model

---

## 24. 这个项目的 Tool Calling 是怎么做的？

项目里有一个 `AgentToolRegistry`，对外暴露两个核心能力：

- `ToolDefinitions()`
- `Execute(...)`

`ToolDefinitions()` 负责把工具 schema 暴露给模型，包括工具名、描述和参数结构；`Execute(...)` 负责在模型返回 `tool_calls` 以后，真正执行对应工具。

当前项目注册了 4 个核心工具：

- `search_knowledge`
- `generate_summary`
- `submit_feedback`
- `knowledge_stats`

然后 ChatService 在发起 `CreateChatCompletion(...)` 时，会把这些 tools 和 `tool_choice=auto` 一起交给模型，让模型自己决定当前问题要不要调用工具。

---

## 25. Tool Calling 和 ReAct 在这个项目里的区别是什么？

Tool Calling 只是说明模型具备“调用工具”的能力。

ReAct 更强调的是一整套多轮循环：

- 模型先判断要不要调工具
- 如果调了工具，就执行工具
- 工具结果作为 observation 回填
- 模型再基于 observation 决定下一步

这个项目里真正实现 ReAct 的核心函数是 `runAgentLoop(...)`。

所以我会这样区分：

- Tool Calling：模型能调工具
- ReAct：模型能基于工具结果继续多轮 act + observation

---

## 26. 这个项目的 ReAct 主循环是怎么工作的？

`runAgentLoop(...)` 先调用 `composeAgentMessages(...)` 组装初始上下文，然后创建一个 `AgentBudget`，限制最大迭代轮次和 token budget。

进入循环以后，每一轮都会带着当前 messages 和 tools 调一次非流式 `CreateChatCompletion(...)`。如果模型没有返回 `tool_calls`，说明这一轮可以结束，后面会进入最终答案输出阶段；如果模型返回了 `tool_calls`，就会进入 `executeToolCalls(...)`，真正去调用工具。

工具结果执行完以后，会被包装成 `role=tool` 的 message 回填到 `messages` 数组里，然后下一轮模型再继续基于这些 tool messages 推理。

所以这条链本质上就是：

assistant(tool_calls) -> tool -> assistant -> tool -> assistant(final)

---

## 27. 为什么最终答案还要单独流式输出一次？

因为这个项目把“Agent 决策阶段”和“用户看到的最终回答阶段”拆开了。

在 ReAct 决策阶段，系统更关注：

- 模型要不要调工具
- 调了哪些工具
- 工具结果怎么回填
- 预算有没有超限

这部分用非流式 completion 更容易控制。

等到 ReAct 循环结束以后，再单独把最终上下文交给 `deliverFinalAnswer(...)`，重新发起一次流式输出。这样可以同时兼顾：

- Agent 运行时控制
- 前端用户体验

---

## 28. 这个项目的 Memory 是怎么做的？

我会把它拆成四层。

第一层是原始会话历史，存在 Redis 里，保存的是完整 Agent trace，而不只是用户和助手的纯文本。

第二层是短期记忆。历史过长时，`BuildContextMessages(...)` 会按 token budget 把较早历史压缩成摘要，同时保留最近原始消息。

第三层是长期记忆。每轮问答结束后，系统会把 question 和 answer 提炼成 `preference / fact / conclusion` 三类结构化条目，然后存在 Redis List 里。后面新问题进来时，再按 query 相关性召回最相关的长期记忆。

第四层是 feedback guidance。最近反馈会被整理成 system guidance，一起注入当前轮上下文。

所以这个项目的 Memory 不是简单“存聊天记录”，而是“历史、摘要、长期记忆、反馈”一起参与后续推理。

---

## 29. Redis 在这个项目里用了哪些数据结构？

这个项目里 Redis 不止是做缓存。

我目前看到的主要有几类：

- String：保存 conversation history 的整段 JSON。
- Bitmap：保存上传分片状态，支撑断点续传。
- List：保存长期记忆、feedback 事件流、工具调用埋点事件。
- Hash：保存 feedback 的原始归档记录，以及我后面补的工具指标汇总。

其中比较值得讲的是两类：

第一，上传状态用 bitmap，特别适合标记 chunk 是否已上传；

第二，feedback 用了 Hash + List 两层结构，其中真正被后续逻辑消费的是 List，MemoryService 和 SearchService 都是从 `feedback_events:{userID}` 这个事件流里读最近反馈。

---

## 30. feedback 里的 Hash 和 List 分别是怎么用的？

`submit_feedback` 里会把反馈写两层 Redis。

第一层是 Hash，key 类似 `feedback:{userID}`，field 是时间戳，value 是简化后的 `rating/reason` 字符串。它更像原始反馈归档。

第二层是 List，key 类似 `feedback_events:{userID}`，每个元素是一条 JSON 格式的 `FeedbackEntry`。这层才是真正被后续主链消费的数据源。

后面：

- MemoryService 会通过 `LRange` 读取最近 feedback event，生成 `feedback guidance` 注入 prompt。
- SearchService 也会通过 `LRange` 读取同一批 feedback event，把它们转换成 retrieval 调参信号。

所以从当前真实代码看：

- Hash 是写了但没发现读取方
- List 是主链真正依赖的反馈事件流

---

## 31. 这个项目前端为什么能展示 Agent 执行过程？

因为后端通过 WebSocket 发的不是一段普通字符串，而是一组结构化事件。

主要包括：

- `tool_status`
- `tool_observation`
- `chunk`
- `references`
- `completion`

前端发送问题以后，会先 push 一条空的 assistant message 占位，后面所有流式正文、timeline、references 都往这条消息上补。`tool_status` 和 `tool_observation` 会被前端组装成 timeline step，`chunk` 会不断追加到 assistant content，`references` 会单独展示成可点击的来源卡片。

所以这个页面不只是“显示答案”，而是在可视化 Agent 的执行轨迹。

---

## 32. 这个项目遇到过什么比较典型的问题？

我印象比较深的有两个。

第一个问题是“文件在文档列表里可见，但搜索查不到内容”。后来排查发现这不是上传 bug，而是异步建库链还没走完，或者中间某一步失败了。这个问题让我更明确地区分了上传成功、建库成功和可检索成功。

第二个问题是“点击引用来源打不开文件”。后来顺着前端来源跳转链路往后查，发现上传合并后的真实对象路径是 `merged/{fileName}`，但下载和预览逻辑一开始拼的却是 `uploads/{userID}/{fileName}`，导致 MinIO 返回 `NoSuchKey`。这个问题本质上是上传链和文档访问链的对象 key 约定不一致，后来把下载和预览统一改成读取 `merged/{fileName}` 就修掉了。

---

## 33. 这个项目有没有做评估？

如果面试官问这个问题，我会诚实回答：

当前项目主要是把完整工程链路落地了，我已经把离线检索评估和工具调用埋点补进项目里，但还没有在足够规模的数据集上跑出正式可对外汇报的数字，所以我不会虚构具体指标。

现在我补的两类评估分别是：

第一类是检索评估。基于人工标注的问题集，统计 `hit rate`、`recall@k` 和 `MRR`，用来比较不同 embedding、不同 topK、不同 Hybrid Search 参数的效果。

第二类是 Agent 工具评估。运行期会埋点统计：

- `requested_total`
- `success_total`
- `failed_total`
- `timeout_total`

所以这个项目已经具备评估能力，但我不会在没跑出真实结果前先写漂亮数字。

---

## 34. 如果后续继续优化这个项目，你会优先做什么？

我会优先做四件事。

第一，优化文本切块。当前固定窗口方案能跑通，但不是语义质量最优方案，后面会考虑按段落、标题和句子做结构感知切块。

第二，补完整评测集。把检索效果、工具成功率、首 token 延迟这些指标真正跑出来，而不是只停留在工程功能验证层面。

第三，继续调 Hybrid Search 参数。比如 topK、recallK、phrase boost、rescore weight、StrictRelevance 的策略，都可以结合评测进一步优化。

第四，扩展 Agent 工具和路由策略。当前工具已经能跑通搜索、摘要、反馈和统计，但如果后续业务更复杂，还可以把工具注册和意图路由做得更统一、更可扩展。

---

## 35. 你如何做这个项目的自我介绍？

面试官您好，我主要做的是一个基于 Go 的 Agentic RAG 知识库问答系统。这个项目不是单纯调大模型接口，而是从文档上传、异步建库、Hybrid Search，一直到 Tool Calling、ReAct、多轮记忆和前端执行轨迹可视化，整条链都打通了。

在上传侧，我做了分片上传、断点续传、MinIO 文件合并和 Kafka 异步建库；在检索侧，我实现了 KNN 向量召回加关键词匹配、短语增强和 rescore 的 Hybrid Search，并且把权限过滤前置到检索层；在问答侧，我结合 Tool Calling 和 ReAct，让模型能够按需调用搜索、摘要、反馈和统计工具；在会话管理上，我还做了短期记忆、长期记忆和 feedback guidance，让系统具备多轮上下文延续能力；前端则通过 WebSocket 把工具执行状态、引用来源和最终答案实时展示出来。

我觉得这个项目对我最大的价值，是让我把 Go 后端工程能力和 AI 应用落地真正结合起来了。

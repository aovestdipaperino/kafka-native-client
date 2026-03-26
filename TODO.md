# TODO — Deferred Features

## Transactions
- [ ] `initTransactions()` — InitProducerIdRequest with transactional_id
- [ ] `beginTransaction()` — local state transition
- [ ] `sendOffsetsToTransaction()` — AddOffsetsToTxnRequest + TxnOffsetCommitRequest
- [ ] `commitTransaction()` — EndTxnRequest with COMMIT
- [ ] `abortTransaction()` — EndTxnRequest with ABORT
- [ ] TransactionManager state machine (Java: `TransactionManager.java`)

## Security
- [ ] SSL/TLS connections (rustls or native-tls)
- [ ] SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER)

## Interceptors
- [ ] ProducerInterceptor trait (onSend, onAcknowledgement)
- [ ] ConsumerInterceptor trait (onConsume, onCommit)

## Partition Assignors
- [ ] Cooperative sticky assignor
- [ ] Custom assignor trait

## Share Consumer
- [ ] KafkaShareConsumer (share group protocol)

## Compression
- [ ] Custom compressor/decompressor plugin API

## Advanced
- [ ] Consumer regex subscription
- [ ] Log truncation detection and handling
- [ ] Client-side telemetry reporting
- [ ] Sender retry logic with exponential backoff
- [ ] Rebalance listener invocation in GroupCoordinator
- [ ] Metadata-aware range assignment (use actual partition counts)

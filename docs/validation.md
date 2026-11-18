# Validação real — Audit MVP de eventos

Executada em 15/11/2026 no Windows PowerShell, Node v22.14.0, npm 10.9.2. Somente arquivos de agrotrac-audit foram modificados; Core foi usado como produtor real de fixtures com opt-in e não teve sua suíte reexecutada. Sem commit, push, alteração de credenciais/roles/Compose ou limpeza de dados.

## Comandos finais e resultados

A partir de agrotrac-audit, com infra healthy:

```powershell
docker start agrotrac-core-test-relay-cf6006
$env:AUDIT_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
$env:AUDIT_E2E_CORE_PATH = (Resolve-Path '../agrotrac-core').Path
$env:PGHOST = '127.0.0.1'
$env:PGPORT = '15432'
npm run build
npm run typecheck
npm test
npm run test:integration
docker stop agrotrac-core-test-relay-cf6006
```

| Comando | Resultado final real |
| --- | --- |
| npm run build | exit 0; tsc gerou dist/ |
| npm run typecheck | exit 0; inclui testes |
| npm test | exit 0; 6 aprovados, 0 falhas, 0 skipped; 244 ms |
| npm run test:integration | exit 0; 9 cenários + teste pai = 10 aprovados no runner, 0 falhas, 0 skipped; 4050 ms |

Unidade usa adapter in-memory e não precisa de Docker. Integração não foi pulada: usou Fastify em portas efêmeras, fetch HTTP por TCP, PostgreSQL real em DOIS bancos e RabbitMQ real. Ausência do opt-in Core, conexão ou broker falha o comando.

## Recursos finais preservados

| Recurso | Identificador |
| --- | --- |
| Banco Audit / schema | `agrotrac_audit` / `audit_it_1794771163519_08124f12` |
| Banco Core de fixtures / schema | `agrotrac_core` / `core_audit_it_1794771163519_08124f12` |
| Exchange de eventos topic durável | `agrotrac.audit.test.aadb97ac97a9400687ba14a340ea784a` |
| Fila de entrada durável Audit | `agrotrac.audit.test.aadb97ac97a9400687ba14a340ea784a.input` |
| Exchange de quarentena direct durável | `agrotrac.audit.test.aadb97ac97a9400687ba14a340ea784a.quarantine` |
| Fila de quarentena durável | `agrotrac.audit.test.aadb97ac97a9400687ba14a340ea784a.rejected` |
| Fila Live de fixtures do publisher Core | `agrotrac.audit.test.aadb97ac97a9400687ba14a340ea784a.live` |
| Fila do cenário de reconexão | `agrotrac.audit.test.aadb97ac97a9400687ba14a340ea784a.input.reconnect` |
| Reserva: event_id Core real | `6e03eb30-e594-4779-83ec-6b07e98998aa` |

Contagem final do ledger Audit: **8 linhas distintas**, sem duplicação por replay. Outbox Core de fixtures publicou **3 eventos** (cadastro silo, cadastro janela, reserva). Fila de quarentena terminou com **6 mensagens prontas preservadas**, conferidas após fechar o canal observador; suas mensagens foram inspecionadas com ACK automático desabilitado e sem emitir ACK manual, sendo recolocadas na fila pelo fechamento, sem remoção. Fila Live de fixtures não foi consumida.

Stats locais finais do consumidor principal: acked=35, inserted=7, duplicate=22, quarantined=6, sqlRetries=2, reconnects=3, lastError=null. `inserted` conta entregas cujo ACK concluiu no processo, não total de linhas do ledger: uma inserção foi commitada antes de uma falha proposital de ACK. O consumidor do cenário de reconexão também recebeu uma entrega e a deduplicação funcionou entre consumidores.

Inspeção inicial de information_schema em agrotrac_audit encontrou **nenhuma tabela de aplicação** antes da primeira migração; as execuções finais encontraram apenas fixtures das execuções anteriores. O banco foi inspecionado antes de cada novo schema. `requireNew` recusou reutilização do schema e a reaplicação da migração reconhecida foi testada. Nenhuma escrita em schemas preexistentes, nenhum DROP/TRUNCATE/DELETE SQL, nenhuma exclusão de fila, container ou volume foi executada.

## Evidências por cenário

| Cenário | Resultado observado e assertado |
| --- | --- |
| Core → RabbitMQ → Audit | POST HTTP reais criaram silo, janela e reserva no Core; seu OutboxWorker publicou 3 eventos com publisher real; Audit persistiu no outro banco; GET por event_id retornou exatamente o envelope original e booking_id |
| Replay sequencial e concorrente | 1 replay sequencial + 20 mensagens simultâneas via broker, com chaves JSON reordenadas, e mais 10 chamadas SQL concorrentes do caso de uso; **1 linha** para o event_id da reserva |
| Falha antes do COMMIT | hook depois do INSERT e antes do COMMIT lançou falha técnica em **3 tentativas**; SQL executou rollback real, count=0 e nenhum ACK; sessão fechou com backoff; ao recuperar, entrega veio com redelivered=true e gerou **1 linha commitada** |
| COMMIT seguido de falha de ACK | hook antes de ACK derrubou a sessão; consulta SQL já via 1 linha e mapa de ACKs permanecia vazio; reconexão trouxe redelivered=true e caminho duplicate; ACK posterior, ainda **1 linha** |
| JSON inválido | bytes `{broken` chegaram à quarentena persistente como raw_base64 íntegro e código INVALID_JSON; gate após confirm real mostrou que ACK original ainda não ocorrera; liberado o gate, ACK foi registrado |
| Versão não suportada | version 9.0.0 foi confirmada em quarentena com UNSUPPORTED_VERSION, sem linha no ledger |
| Mesmo ID com outro conteúdo | payload conflitante foi para quarentena com EVENT_ID_CONFLICT; count original permaneceu 1 e GET confirmou envelope original preservado |
| Falha de rota da quarentena | removido SOMENTE binding da fila de teste isolada; mandatory/return fez a transferência falhar apesar do confirm do broker; nenhum ACK original; reconexão redeclarou binding e a mensagem foi quarentenada e ACKada após confirmação |
| Envelope estrutural/rota inválidos | payload array e type divergente da routing key geraram INVALID_ENVELOPE e ROUTING_KEY_MISMATCH; nenhuma linha para esses IDs, ambas mensagens preservadas na quarentena |
| HTTP/filtros | filtros type/correlation_id retornaram 2 fixtures; combinação de event_id com type divergente retornou vazio; paginação limit=2 percorreu **7 eventos únicos** existentes naquele momento, sem duplicações; cursor textual e next_cursor conferidos |
| Queries inválidas | 9 combinações inválidas (limites, cursor negativo/overflow, UUIDs, type, campo desconhecido) retornaram 400; header correlation inválido retornou 400; rota de pesagens e POST de eventos retornaram 404 |
| Broker indisponível/reconexão | proxy TCP próprio recusou inicialmente conexões e permitiu recuperação após pelo menos 2 tentativas com backoff; depois encaminhou para RabbitMQ real; novo evento chegou a dois consumidores e gerou 1 linha; conexões estabelecidas do proxy foram encerradas e consumidor reconectou, sem parar broker compartilhado |
| Encerramento | ambos consumidores, aplicações HTTP, proxy TCP, publisher Core, canais/conexões AMQP e pools SQL foram fechados; runner terminou naturalmente com exit 0 |

Unidade também validou: producer Core/Live e prefixo type correspondente, UTC/calendário, UUID canônico, versão desconhecida, números inseguros, NUL/Unicode inválido para JSONB, payload cíclico/profundo, comparação canônica recursiva e ordem de arrays, preservação do original em conflito e paginação limitada.

## Simulações e limites das evidências

- Falha SQL foi injetada dentro da transação PostgreSQL real antes do COMMIT; não foi necessário parar o servidor compartilhado. O teste confirmou rollback e ausência de ACK. A configuração real usa 3 tentativas com backoff de 250/500ms; testes usam 40/80ms e reconexão 300ms para não demorar, com os mesmos caminhos de código.
- Falha de ACK foi simulada entre o retorno de COMMIT e channel.ack, fechando a própria sessão; a reentrega foi real do RabbitMQ. Não se afirma que ACK de consumidor tenha publisher confirm.
- A garantia é transporte pelo menos uma vez e efeito idempotente no ledger por UNIQUE(event_id). Não se afirma exactly-once. Queda após confirm de quarentena e antes de ACK original pode duplicar a quarentena.
- Ciclos de recuperação SQL podem continuar durante outage prolongado, sempre com número local de tentativas limitado e espera entre sessões; não há descarte após N ciclos. Não foi feito teste de carga prolongado ou benchmark de throughput.
- Advisory lock por schema serializa appends curtos para cursor seguir ordem de COMMIT. Isso limita throughput e deverá ser revisto com medições futuras. O consumidor mantém prefetch limitado e não faz I/O AMQP dentro da transação SQL.
- Nenhuma fila operacional foi consumida ou usada para publicar fixtures. A topologia de teste usa os mesmos bindings `agrotrac.core.#` e `agrotrac.live.#`, mas nomes UUID isolados; o cenário de falha de binding alterou apenas seu recurso novo próprio.
- Ledger é append-only pelas operações da aplicação. A role de produção restrita ainda precisa ser provisionada, sem alterar permissões nesta entrega. Nenhum teste afirma resistência a UPDATE/DELETE executado por administrador do banco.
- Pesagens, fotos, auth/Gateway, projeções Live e redrive administrativo de quarentena estão fora do escopo entregue. Consulta HTTP fica em 127.0.0.1 por padrão. Conteúdo do ledger/quarentena pode conter dados pessoais e precisa de controle de acesso antes de exposição externa.

## Histórico de execução e infraestrutura preservada

Uma tentativa inicial de configuração E2E falhou antes de aplicar qualquer DDL: copiar as opções do pool com spread omitia a propriedade password não enumerável do pg. O teste foi corrigido para repassar essa propriedade explicitamente em memória, sem imprimi-la; o processo de teste daquela falha foi encerrado e não foi contado como aprovação. Esse problema não afetava a configuração de produção, que já passava senha explicitamente.

Execuções anteriores completas preservadas: schemas `audit_it_1794770744409_7d0f929a` / `core_audit_it_1794770744409_7d0f929a`, e `audit_it_1794770871158_627a31e2` / `core_audit_it_1794770871158_627a31e2`, com exchanges/filas UUID próprias impressas nos respectivos runs. O primeiro run completo anterior ao ajuste de preservação ACKou suas observações de quarentena; o segundo e o final preservam seis mensagens em suas filas. Não houve limpeza desses recursos ou dos schemas.

O PostgreSQL host 5432 conflita com processo Windows; foi usado somente o relay próprio autorizado `agrotrac-core-test-relay-cf6006` em 127.0.0.1:15432, já existente, ligado a agrotrac-infra_default e encaminhando postgres:5432. Ele foi iniciado para os testes e parado ao concluir, preservando o container sem volume; os três serviços da infra continuaram healthy. Credenciais foram lidas apenas pelo caminho explicitamente configurado ao env local da infra e nunca foram impressas ou gravadas em código/docs. Nenhum serviço de aplicação foi deixado rodando.

## Revisão complementar — pool saturado e Consumer.stop (15/11/2026)

O achado independente era condicional: PgLedger.append aguarda pool.connect antes de entrar na transação e Consumer.stop aguarda tarefas em curso. Inspeção de config.ts confirmou que **os dois ramos de configuração já especificavam connectionTimeoutMillis=3000**, além de statement_timeout=5000 e query_timeout=6000. O teste real abaixo demonstrou que connectionTimeoutMillis também limita espera de checkout de um pool saturado neste driver, refutando a hipótese de espera sem prazo nesse cenário configurado. Não foi necessário alterar código de produção, retries ou arquitetura.

Adicionado `tests/integration/pool-shutdown.test.ts` e script `npm run test:integration:pool`. O arquivo também é incluído pelo glob de test:integration, mas pode rodar sozinho sem importar Core ou exigir AUDIT_E2E_CORE_PATH. Usa a configuração real opt-in para criar um pool observador e um pool fixture no mesmo banco, copiando as credenciais somente em memória. Confere os timeouts reais de produção e aplica no fixture **min=1, max=1, connectionTimeoutMillis=250ms**, pelo mesmo caminho PgLedger.append e mesmo driver pg. O único cliente real é adquirido e mantido fora do pool; waitingCount=1, totalCount=1 e idleCount=0 são assertados. Nenhum hook/fake simula a espera do pool.

Comandos da verificação final desta revisão:

```powershell
docker start agrotrac-core-test-relay-cf6006
$env:AUDIT_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
$env:PGHOST = '127.0.0.1'
$env:PGPORT = '15432'
npm run build
npm run typecheck
npm test
npm run test:integration:pool
docker stop agrotrac-core-test-relay-cf6006
```

| Verificação desta revisão | Resultado real final |
| --- | --- |
| Build | exit 0 |
| Typecheck | exit 0, incluindo novo teste |
| Unidade | 6 aprovados, 0 falhas/skips; 246 ms |
| Integração focada | 2 cenários + pai = 3 aprovados no runner, 0 falhas/skips; 1212 ms |
| Checkout saturado | **258 ms** até rejeição; waitingCount voltou a 0; cliente continuou retido, mesmo backend PID 2955; nenhuma linha persistida |
| Consumer.stop com checkout pendente | **255 ms** até resolver enquanto cliente ainda estava retido; 0 ACKs, 0 inserts/linhas; **1 mensagem pronta no broker** depois do fechamento |
| Recuperação | só depois de stop resolver o cliente foi liberado; outro AuditConsumer recebeu redelivered=true, persistiu **1 linha**, consulta separada no hook antes do ACK viu essa linha já commitada, e houve **1 ACK**; fila de entrada ficou vazia |

As durações são medições de parede que incluem escalonamento e verificação do fixture; asserções exigem checkout entre 220ms e 1500ms e stop abaixo de 1500ms, com guard externo de 2s. O cliente não foi liberado para ajudar stop a concluir: ele permaneceu retido durante toda a espera e foi liberado somente após asserções de ausência de ACK e ausência de persistência. Um pool observador separado permitiu consultar o ledger durante a saturação sem competir pelo cliente retido.

Recursos finais preservados exclusivamente desta revisão:

- Banco `agrotrac_audit`, schema **`audit_pool_it_1794771603332_bd5dbe16`**, inspecionado vazio antes da migração aditiva; contagem final **1 evento**.
- Exchange topic durável `agrotrac.audit.pool.bea1542b45d544e7bbc65e613463b8d6`.
- Fila durável de entrada `agrotrac.audit.pool.bea1542b45d544e7bbc65e613463b8d6.input`.
- Exchange direct durável de quarentena `agrotrac.audit.pool.bea1542b45d544e7bbc65e613463b8d6.quarantine` e fila durável `agrotrac.audit.pool.bea1542b45d544e7bbc65e613463b8d6.rejected` (sem rejeições neste teste).
- Event_id recuperado `d45233ef-ae28-455e-8528-0ea94633324b`.

Uma primeira execução focada também concluiu, com checkout **259ms**, stop **259ms**, 0 ACK/0 linhas durante saturação e recuperação com 1 linha. Preservou schema `audit_pool_it_1794771555960_a0d15081` e recursos do prefixo `agrotrac.audit.pool.57ae32624b2748aa9ea79a0aae8f4c14`. Não houve falha reproduzida nem limpeza desses recursos. O relay autorizado foi parado novamente ao terminar; banco, broker e Redis compartilhados continuaram healthy. Não foram alterados Core, infra, credenciais, roles ou tabelas/filas preexistentes. Não houve commit/push.

**Limites e avaliação do achado menor:** a evidência cobre especificamente tarefa pendente em checkout com o pool configurado e rede de infraestrutura disponível, não um prazo global absoluto para qualquer combinação de consulta SQL em execução, partição de rede ou driver substituído. Callers que injetem um Pool próprio sem timeout não têm a mesma garantia; o processo de produção usa configuration(). A suíte E2E anterior Audit/Core não foi reexecutada nesta revisão sem alterações runtime, e a suíte Core não foi executada; resultados anteriores acima permanecem históricos.

Por inspeção do Consumer.run, sucesso de uma entrega ACKada zera o contador de backoff; numa falha parcial, sucessos intercalados podem manter reconexões próximas ao piso de **1s**, em vez de acumular até 30s. Isso limita a escalada do backoff, mas ainda preserva a espera mínima, o prefetch limitado, 3 tentativas locais e mensagens sem ACK quando não persistidas; não é requeue imediato nem defeito de integridade demonstrado. Foi mantido e documentado como limite operacional do MVP; histerese por intervalo saudável/jitter é melhoria futura. Esta avaliação do reset é por leitura de código, sem alegar novo teste de carga ou benchmark de falha parcial.

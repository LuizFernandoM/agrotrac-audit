# Contratos Audit — ledger de eventos MVP

Este documento cobre ingestão RabbitMQ e consulta do ledger. Pesagens e fotos foram adicionadas na etapa v2 descrita em weighings-contract.md, sem alterar o envelope/consumidor de eventos. Relatórios fiscais, autenticação e Gateway seguem fora desta etapa. Audit não importa Core em produção nem lê tabelas Core. Seu banco padrão é `agrotrac_audit`, separado de `agrotrac_core`.

## Envelope de entrada

JSON UTF-8, com exatamente os campos abaixo, compatível com o envelope real emitido pelo Core:

```typescript
{
  event_id: UUIDv4,
  type: string,
  version: '1.0.0',
  producer: 'agrotrac-core' | 'agrotrac-live',
  occurred_at: UTC,
  correlation_id: UUIDv4,
  payload: Record<string, JSONValue>
}
```

UUIDs são v4 canônicos minúsculos. UTC aceita `YYYY-MM-DDTHH:mm:ssZ` ou `YYYY-MM-DDTHH:mm:ss.sssZ`, com validação real de calendário. As strings do envelope são preservadas; duas representações textuais distintas do mesmo instante ainda são conteúdos distintos para idempotência.

`type` tem até 100 caracteres, padrão `agrotrac.(core|live)(.[a-z0-9_]+)+`, prefixo correspondente ao producer e deve coincidir com a routing key AMQP. Audit preserva os snapshots e versões de agregados do payload Core, mas não aplica projeções nem inventa schemas para eventos futuros do Live. A versão semântica suportada é exatamente `1.0.0`; outras versões são quarentenadas.

Payload deve ser objeto JSON, não array/null; aceita JSON aninhado até profundidade 32. Números inteiros precisam ser seguros em JavaScript e demais números devem ser finitos. Strings/chaves com NUL ou substitutos Unicode isolados são rejeitadas porque JSONB PostgreSQL não as armazena. Mensagem acima de 1 MiB ou UTF-8 inválido é rejeitada para quarentena, preservando os bytes originais. Não há coerção de campos de massa ou interpretação de pesagem.

## Persistência e idempotência

`Audit.ingest` valida o envelope e usa a porta `Ledger.append`. O adapter PostgreSQL abre transação READ COMMITTED, insere com `ON CONFLICT(event_id) DO NOTHING`, e compara o envelope existente com `$envelope::jsonb` quando já existe. `UNIQUE(event_id)` impede duplicação; a comparação JSONB ignora ordem de chaves em todos os níveis e preserva ordem de arrays e valores.

- Evento novo: INSERT e COMMIT, depois ACK manual.
- Mesmo ID e JSONB igual: não insere nem altera nada, conclui transação e ACK.
- Mesmo ID e conteúdo diferente: `EVENT_ID_CONFLICT`; preserva o ledger original e transfere o conteúdo conflitante para quarentena confirmada antes do ACK original.
- Falha antes do COMMIT: ROLLBACK; nenhum ACK.
- COMMIT seguido de falha de ACK: a reentrega encontra o registro e é idempotente.

Aplicação emite somente INSERT/SELECT no ledger, sem UPDATE/DELETE. A migração não instala triggers de bloqueio nem altera roles, senhas ou permissões. O papel atual da infra permite operações além das usadas pela aplicação: provisionar uma role de produção com INSERT/SELECT e permissões da sequência, sem UPDATE/DELETE/DDL, permanece pendente e exige autorização própria. Não se afirma imutabilidade contra administradores do banco.

DDL aditivo em `src/infra/migration.ts`, sempre em schema explícito não público/não sistema: `event_ledger` e `audit_migrations`. Campos do ledger: sequence BIGSERIAL, event_id UUID UNIQUE, type, correlation_id UUID, envelope JSONB e received_at TIMESTAMPTZ. CHECKs mantêm vínculo entre colunas indexadas e envelope, presença dos campos, versão/produtor e payload objeto. Índices atendem type e correlation_id, além do UNIQUE event_id. O domínio faz a validação completa, inclusive UUID/UTC.

Um advisory lock transacional por schema serializa os INSERTs curtos para que a sequência do cursor siga a ordem dos COMMITs. Há custo de throughput aceito neste MVP; nenhum I/O AMQP ocorre sob esse lock. Transações têm lock_timeout 3s e statement_timeout 5s. Conexão PostgreSQL tem timeout de 3s, query timeout de 6s e pool de até 10 clientes. Sequências podem ter lacunas por rollback/replay; não representam contagem de eventos.

## Consumo, falhas e encerramento

Topologia canônica:

| Recurso | Configuração |
| --- | --- |
| agrotrac.events | exchange topic, durable, autoDelete false |
| agrotrac.audit.events | fila durable, autoDelete false, sem novos x-arguments |
| bindings da entrada | `agrotrac.core.#` e `agrotrac.live.#` explícitos |
| agrotrac.audit.quarantine | exchange direct própria, durable, autoDelete false |
| agrotrac.audit.quarantine.events | fila própria durable, binding `rejected`, autoDelete false |

Nomes são injetáveis/configuráveis. Não são adicionados TTL, DLX ou outros argumentos à fila canônica existente. Incompatibilidade de declaração é falha da sessão, sem apagar/recriar a fila ou confirmar mensagens. Topologia de quarentena é separada da exchange operacional.

Prefetch padrão 8 por consumidor, ACK manual. Em falha técnica de persistência: no máximo 3 tentativas locais por entrega, esperas de 250ms e 500ms (teto 5s no cálculo local). Se esgotar, cancela consumo, drena tarefas, fecha a conexão própria e deixa mensagens sem ACK voltarem ao broker. Reconecta com backoff de 1s exponencial até 30s; ACK bem-sucedido reinicia esse backoff. Ciclos continuam até recuperação, sem descarte e sem `nack(requeue=true)` em loop rápido. Não há limite global que abandone eventos durante indisponibilidade prolongada. Timeout SQL/AMQP e prefetch limitam trabalho por ciclo.

Publicação de quarentena usa canal confirm separado, mensagens persistentes, mandatory/return e deadline padrão 5s. Não considera confirm como prova de rota: return é falha mesmo com ACK do broker. O consumidor original só confirma depois da confirmação roteada da quarentena. Se publicação/rota/confirm falhar, não há ACK original; a sessão reconecta com backoff e a mensagem permanece disponível para nova tentativa.

Reconexão redeclara a topologia e retoma consumo. Encerramento cancela novas entregas, aborta esperas, aguarda SQL em curso, fecha conexão AMQP própria (com deadline e destruição do próprio socket quando necessário) e fecha o pool. Conexões que resolvem depois de timeout são encerradas. Falhas e status usam códigos fixos sanitizados; nenhuma credencial ou corpo de evento é enviado a logs da aplicação.

Garantia: transporte pelo menos uma vez; ledger idempotente por event_id. Não há promessa de exactly-once no transporte. Se houver falha depois do confirm da quarentena e antes do ACK original, a quarentena também pode conter duplicatas; `source.message_id`, routing key e bytes originais permitem correlação. Quarentena não é eliminada automaticamente nem reprocessada como evento válido sem análise/correção da origem; redrive administrativo fica fora desta entrega.

## Registro de quarentena

```typescript
{
  quarantine_id: UUIDv4,
  error: { code: string },
  quarantined_at: UTC,
  source: { exchange: string, routing_key: string, message_id: string | null },
  raw_base64: string
}
```

Códigos permanentes: `INVALID_JSON` (inclui UTF-8 inválido), `INVALID_ENVELOPE`, `UNSUPPORTED_VERSION`, `ROUTING_KEY_MISMATCH`, `MESSAGE_TOO_LARGE` e `EVENT_ID_CONFLICT`. Erros internos SQL, stack traces, senhas e metadados de conexão não são incluídos em `error`. Bytes originais podem conter dados pessoais: acesso à quarentena requer a mesma proteção do ledger, a implementar junto à segurança de produção.

## HTTP local

`GET /v1/audit/events`, bind padrão `127.0.0.1:3003`, sem autenticação. Filtros exatos opcionais e combináveis por AND:

| Query | Validação |
| --- | --- |
| type | mesmo formato de type do envelope |
| event_id | UUID v4 canônico |
| correlation_id | UUID v4 canônico |
| limit | inteiro decimal textual 1–100, padrão 50 |
| cursor | sequência decimal não negativa, até 9223372036854775807, padrão 0 |

Filtros desconhecidos/inválidos retornam 400. Paginação por `sequence > cursor ORDER BY sequence ASC LIMIT limit+1`; `next_cursor` é a sequência do último item retornado quando existe mais um item, senão null. Use cursor opaco como string, sem conversão para Number. O cursor acompanha a ordem local de ingestão, não occurred_at nem ordem global dos produtores. Paginação não fixa um snapshot de futuras inserções; podem surgir páginas adicionais enquanto novos eventos chegam.

Resposta 200:

```typescript
{ items: [{ sequence: string, event: Event, received_at: UTC }], next_cursor: string | null }
```

Erros usam application/json e formato próprio AgroTrac:

```json
{"error":{"code":"INVALID_QUERY","message":"INVALID_QUERY","details":null,"correlation_id":"b3e2a109-7654-4321-abcd-ef0123456789","timestamp":"2026-11-15T19:30:00.000Z"}}
```

`X-Correlation-Id` de resposta é sempre válido; Core/Gateway não são necessários para gerá-lo. Header ausente gera UUID; fornecido inválido retorna 400 `INVALID_CORRELATION_ID` com um novo ID. `INVALID_QUERY` é 400, rota/método ausente é 404 `NOT_FOUND`, falha técnica é 500 `INTERNAL_SERVER_ERROR`. Erros de parsing/limites HTTP são sanitizados como `INVALID_REQUEST`, preservando status 4xx do servidor. Não há endpoints de mutação HTTP do ledger; os endpoints de pesagens/fotos são documentados separadamente em weighings-contract.md.

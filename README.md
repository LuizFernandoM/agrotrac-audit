# AgroTrac Audit — ledger de eventos MVP

Consome eventos do Core e futuro Live pelo RabbitMQ, mantém ledger idempotente e append-only na aplicação em PostgreSQL `agrotrac_audit`, e sincroniza pesagens com fotos JPEG/PNG de romaneios. Node.js 22, TypeScript, Fastify, pg, amqplib, multipart e sharp. Relatórios fiscais e políticas de descarga/retificação ficam para etapa futura.

Contratos de envelope, erros, retries e quarentena: [docs/contracts.md](docs/contracts.md). Resultados reais: [docs/validation.md](docs/validation.md).

## Instalar e validar unidade

```powershell
npm ci
npm run build
npm run typecheck
npm test
```

Unidade roda sem Docker ou serviços externos, usando a porta Ledger com adapter in-memory. Build gera dist/. Código de produção não importa arquivos nem dependências do repo Core.

## Configurar e executar

Não existe descoberta automática de `.env`. Para optar pelo arquivo local da infra:

```powershell
$env:AUDIT_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
$env:AUDIT_DB_SCHEMA = 'agrotrac_audit_mvp'
$env:AUDIT_STORAGE_PATH = [System.IO.Path]::GetFullPath('uploads/romaneios')
$env:PGHOST = '127.0.0.1'
$env:PGPORT = '15432' # relay local quando 5432 pertence ao PostgreSQL Windows
npm run migrate
npm start
```

Banco padrão é **agrotrac_audit**, independentemente de POSTGRES_DB da infra (que aponta ao Core). Configuração de conexão por AUDIT_DATABASE_URL ou PGHOST/PGPORT/PGUSER/PGPASSWORD/AUDIT_PGDATABASE. O arquivo opt-in pode fornecer POSTGRES_USER/POSTGRES_PASSWORD e RABBITMQ_DEFAULT_USER/RABBITMQ_DEFAULT_PASS; variáveis exportadas têm precedência. Não copie senhas reais para exemplos, código, comandos ou logs. Credenciais ausentes falham com erro sanitizado.

Migração é explícita/aditiva v1→v2, em schema próprio obrigatório; recusa public/schemas de sistema, schema desconhecido ocupado e histórico diferente de [1] ou [1,2]. Preserva o ledger e adiciona weighings; schema novo recebe ambas versões. Não cria banco nem altera role/permissões/senhas. O processo principal verifica ledger/pesagens e inicializa storage antes de assinar a fila; não aplica migrations automaticamente. Não use fixtures de tarefas anteriores como ambiente operacional.

| Variável | Padrão/uso |
| --- | --- |
| AUDIT_DB_SCHEMA | obrigatório, schema explícito não público |
| AUDIT_INFRA_ENV_PATH | caminho absoluto opt-in para env local; opcional se conexão configurada por ambiente |
| AUDIT_DATABASE_URL | URI PostgreSQL opcional, nunca imprimir |
| AUDIT_PGDATABASE | agrotrac_audit |
| PGHOST / PGPORT | 127.0.0.1 / 5432; ajustar explicitamente ao relay quando necessário |
| PGUSER / PGPASSWORD | sem segredo padrão; ou nomes POSTGRES_USER/POSTGRES_PASSWORD no arquivo opt-in |
| RABBITMQ_URL | URI opcional; senão derivada das variáveis opt-in, host 127.0.0.1:5672 |
| AUDIT_HOST / AUDIT_PORT | 127.0.0.1 / 3003 |
| AUDIT_EVENTS_EXCHANGE | agrotrac.events, topic durável |
| AUDIT_EVENTS_QUEUE | agrotrac.audit.events, durável, sem novos x-arguments |
| AUDIT_QUARANTINE_EXCHANGE | agrotrac.audit.quarantine, direct durável própria |
| AUDIT_QUARANTINE_QUEUE | agrotrac.audit.quarantine.events, durável própria |

Bindings da entrada são **agrotrac.core.# e agrotrac.live.#**, não `*`. Consumidor tem prefetch 8 e ACK manual somente depois de COMMIT ou confirmação roteada da quarentena. Mudanças de nomes permitem isolar ambientes; não aponte testes para filas operacionais. Não acrescente argumentos à fila canônica existente; incompatibilidades precisam de análise de implantação, sem apagar/recriar recursos.

Sem autenticação pronta, o bind local é intencional; Gateway e autenticação são pendências de segurança antes de exposição externa. A role atual da infra não foi alterada: restrição de produção para INSERT/SELECT na tabela e uso da sequência ainda precisa ser provisionada mediante autorização. O caráter append-only é garantido pelas operações da aplicação, não contra administradores do banco.

## Consulta

```text
GET http://127.0.0.1:3003/v1/audit/events?type=agrotrac.core.booking.created&limit=50
```

Filtros exatos opcionais: type, correlation_id, event_id. Limit 1–100 (padrão 50), cursor decimal textual (padrão 0). Resposta `{items:[{sequence,event,received_at}],next_cursor}`. Use next_cursor na próxima chamada até null; não converta sequence/cursor para Number. Ordem local de ingestão por sequência, independente do horário original do evento. Não existem POST/PUT/PATCH/DELETE para o ledger.

IDs duplicados iguais retornam o registro preservado na ingestão e não geram novas linhas. Mesmo ID com conteúdo diferente é conflito e vai à quarentena, sem sobrescrever o original. Igualdade é JSONB, independente de ordem das chaves. Transporte continua pelo menos uma vez, sem promessa de exactly-once.

## Falha e recuperação

Falha SQL mantém a entrega sem ACK, com 3 tentativas locais e backoff; ao esgotar, o consumidor fecha sua sessão e reconecta com espera exponencial de 1s até 30s, preservando a mensagem no broker. Não há descarte após N ciclos nem requeue em hot loop. Mensagem inválida, versão não suportada e conflito vão para quarentena persistente própria com confirm/mandatory/return antes do ACK original; se essa transferência falhar, a mensagem original também permanece sem ACK.

Quarentena preserva bytes originais em raw_base64 e código sanitizado. Acesso precisa de proteção por conter dados pessoais; não imprima corpos em logs. Não há limpeza nem redrive automático de quarentena: investigar erro e corrigir a origem é etapa operacional; tooling administrativo futuro deve preservar rastreabilidade. Queda após confirm e antes do ACK pode duplicar registros de quarentena, identificáveis pela origem. SIGINT/SIGTERM cancelam consumo, drenam operações e liberam conexões/pool.

## Integração real com Core, SQL e RabbitMQ

O teste importa Core **somente com opt-in**, como produtor de fixtures via HTTP e OutboxWorker; não reexecuta sua suíte e não cria dependência runtime em produção. Core precisa ter suas dependências locais instaladas. O teste cria schemas únicos novos nos dois bancos e usa somente exchange/filas UUID isoladas com os mesmos bindings, preservando fixtures e recursos.

No ambiente validado, PostgreSQL Windows ocupa host 5432. O relay próprio já existente pode ser iniciado, sem alterar infra:

```powershell
docker start agrotrac-core-test-relay-cf6006
$env:AUDIT_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
$env:AUDIT_E2E_CORE_PATH = (Resolve-Path '../agrotrac-core').Path
$env:PGHOST = '127.0.0.1'
$env:PGPORT = '15432'
npm run test:integration
docker stop agrotrac-core-test-relay-cf6006
```

O relay encaminha 127.0.0.1:15432 para postgres:5432 na rede agrotrac-infra_default; não contém senha ou volume. Não pare o PostgreSQL Windows, não mude Compose/.env e não exclua container, dados, filas ou volumes. Após a validação desta entrega o relay foi parado e preservado.

Integração sem opt-in, banco ou broker falha; nunca é silenciosamente pulada. AUDIT_DB_SCHEMA operacional é ignorado pelo teste, que cria `audit_it_<timestamp>_<id>` e `core_audit_it_<timestamp>_<id>` novos e imprime nomes seguros. AUDIT_E2E_CORE_DATABASE opcional seleciona o banco Core de fixtures, padrão agrotrac_core; o teste exige Audit em agrotrac_audit e banco distinto. O adapter Audit não acessa schema Core. Nenhum DROP/TRUNCATE/DELETE SQL é emitido; observações de quarentena são recolocadas pela liberação do canal de teste, deixando mensagens rejeitadas preservadas na fila durável.

## Estrutura e limites

### Revisão de saturação do pool e encerramento

`npm run test:integration:pool` executa a regressão focada com PostgreSQL/RabbitMQ reais, sem importar Core e sem exigir AUDIT_E2E_CORE_PATH. Configure AUDIT_INFRA_ENV_PATH, PGHOST e PGPORT como acima e inicie o relay quando necessário. O teste cria schema e topologia únicos, retém o único cliente de um pool min=max=1, verifica prazo de checkout e Consumer.stop com uma entrega pendente, e depois confirma reentrega com COMMIT antes do ACK. Preserva schema/filas e não para banco ou broker. Esse arquivo também integra o comando test:integration completo.

A configuração de produção já define connectionTimeoutMillis=3000 nos dois modos de conexão: o prazo também limita checkout quando o pool está saturado, não apenas conexão TCP inicial. A regressão usa 250ms no mesmo driver/caminho PgLedger e mantém o cliente retido até stop resolver; não foi necessário mudar código de produção para o possível bloqueador. Tempos e recursos observados estão na seção de revisão de validation.md. Isso verifica checkout pendente, sem prometer um prazo global absoluto para qualquer combinação de SQL/rede em andamento.

O reset do backoff após uma entrega ACKada foi mantido como política de recuperação do MVP. Em falha parcial, mensagens bem-sucedidas podem manter futuras reconexões próximas do piso de 1s, em vez de aumentar até 30s. Prefetch, tentativas locais e essa espera continuam limitando a pressão; não há requeue imediato ou perda de ACK. Histerese por período saudável/jitter é melhoria operacional futura, não defeito de persistência reproduzido nesta revisão.

`src/domain/event.ts`: Result, envelope e comparação canônica. `src/application/ledger.ts`: casos de uso e porta Ledger. `src/infra/`: PostgreSQL, migração, HTTP, AMQP e consumidor em arquivos separados. `tests/support/memory.ts`: adapter sem serviços externos.

Advisory lock por schema mantém ordem de COMMIT da sequência usada na paginação; limita throughput de gravação e deverá ser reavaliado com benchmark futuro. Não há projeções, auth, redrive administrativo ou garantias de ordem global entre produtores nesta entrega. Evidências anteriores estão em validation.md; pesagens/fotos e regressão completa em weighings-validation.md.

## Pesagens e fotos de romaneios

Contrato completo: [docs/weighings-contract.md](docs/weighings-contract.md). Evidências: [docs/weighings-validation.md](docs/weighings-validation.md).

`POST /v1/audit/weighings/sync` recebe exatamente um campo textual `weighing` com JSON e um arquivo `romaneio` JPEG/PNG, em qualquer ordem. JSON obrigatório: local_id UUID v4, placa, peso_bruto_kg, tara_kg e peso_liquido_kg em inteiros seguros; líquido deve coincidir com bruto−tara. booking_id, captured_at e dados de motorista/observações são opcionais. Ausente/null normaliza estavelmente para null, datas UTC equivalentes normalizam com milissegundos. Captured_at ausente não recebe relógio do servidor. Exemplo sem segredo:

```javascript
import { readFile } from 'node:fs/promises';
const form = new FormData();
form.set('weighing', JSON.stringify({ local_id: '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d', placa: 'BRA2E19', peso_bruto_kg: 42500, tara_kg: 15200, peso_liquido_kg: 27300 }));
form.set('romaneio', new Blob([await readFile('C:/caminho-operador/romaneio.jpg')], { type: 'image/jpeg' }), 'camera.jpg');
const response = await fetch('http://127.0.0.1:3003/v1/audit/weighings/sync', { method: 'POST', body: form });
console.log(response.status, await response.json());
```

201 novo, 200 retry integralmente igual incluindo hash da foto, 409 mesmo ID divergente, preservando original. Valida todo multipart antes de promover/gravar; campos extras/repetidos, JSON desconhecido/duplicado, arquivo falso/truncado/corrompido, tamanho excessivo e upload parcial não confirmam. Decoder completo limitado, CRC PNG e estrutura JPEG complementam magic bytes; não faz OCR nem interpreta a foto.

`GET /v1/audit/weighings` aceita filtros placa/booking_id/local_id e cursor/limit como no ledger. `GET /v1/audit/weighings/:local_id/romaneio` verifica hash/tamanho do arquivo e entrega bytes originais com private,no-store e nosniff. Não retorna paths internos nem oferece URL pública de storage. Sem consulta Core, alteração de reserva/capacidade ou evento novo Audit.

Storage absoluto configurável por AUDIT_STORAGE_PATH, padrão absoluto resolvido de uploads/romaneios no cwd. AUDIT_MAX_FILE_BYTES default 10MiB, máximo 100MiB coerente com CHECK SQL; AUDIT_MAX_JSON_BYTES default/teto 64KiB; AUDIT_MAX_IMAGE_PIXELS default/teto 16777216 e dimensões até 8192. Valores inválidos/paths relativos configurados são recusados.

Raiz e componentes são controlados pelo operador, sem permissão de escrita a usuários não confiáveis: symlink/junction/indireção são rejeitados. Staging exclusivo é preservado; promoção usa hard link exclusivo no mesmo volume para nome derivado do hash, verificando destino existente sem substituí-lo. Arquivo pronto antes de SQL; rollback/conflito pode deixar órfão, nunca remove final compartilhado. Cada retry pode manter outro staging; deduplicação é de arquivos finais. Aplicação nunca reescreve staging completo/final, mas seus hard links compartilham inode e alteração externa de um alias pode corromper ambos. Limpeza/retencão posterior requer autorização.

Há fsync de arquivo e tentativa de fsync de diretório onde suportado; no Windows testado a sincronização de diretório não foi suportada. Não é garantia absoluta contra hardware, volume, remoção ou corrupção externa; erro de integridade não regrava automaticamente dado. Role restrita de produção, autenticação e políticas de retenção/retificação seguem pendentes.

O teste de pesagens cria seu próprio schema v1 e evento antes de v2 e preserva diretorio exclusivo em tests/fixtures/generated, incluindo staging, órfãos, imagens corrompidas e junction controlada de teste. Para executar somente pesagens com a conexão opt-in e relay acima: `npx tsx --test tests/integration/weighings.test.ts`. `npm run test:integration` inclui também ledger/Core E2E via AUDIT_E2E_CORE_PATH e regressão de pool; não repete a suíte Core inteira.

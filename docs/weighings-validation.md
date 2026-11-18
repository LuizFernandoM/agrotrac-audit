# Validação real — pesagens e fotos (v2)

Executada em 15/11/2026, Windows PowerShell, Node 22.14.0 e npm 10.9.2. Somente agrotrac-audit foi editado. Ledger/consumidor AMQP existentes foram preservados; não foi criada publicação de produtor Audit, consulta Core de pesagem, conclusão de reserva ou alteração de capacidade. Nenhum commit, push ou deploy. O histórico anterior em docs/validation.md não foi apagado ou reescrito.

## Comandos e resultado final

Dependências locais adicionadas: @fastify/multipart 10.1.8 e sharp 0.35.5, com package-lock atualizado. A partir de agrotrac-audit:

```powershell
docker start agrotrac-core-test-relay-cf6006
$env:AUDIT_INFRA_ENV_PATH = (Resolve-Path '../agrotrac-infra/.env').Path
$env:PGHOST = '127.0.0.1'
$env:PGPORT = '15432'
$env:AUDIT_E2E_CORE_PATH = (Resolve-Path '../agrotrac-core').Path
npm run build
npm run typecheck
npm test
npm run test:integration
docker stop agrotrac-core-test-relay-cf6006
```

| Comando | Resultado final observado |
| --- | --- |
| Build | exit 0 |
| Typecheck | exit 0, incluindo novos testes e processo HTTP de fixture |
| Unidade | 10 aprovados, 0 falhas, 0 skipped; 279 ms; sem Docker |
| Integração Audit completa | 24 aprovados no runner, 0 falhas, 0 skipped; 5827 ms |

Os 24 testes de integração incluem 9 cenários do ledger, 2 da saturação do pool e 10 de pesagens, mais os 3 testes pais. Nenhum teste anterior foi removido ou relaxado. E2E Core foi usado somente com opt-in como produtor real via HTTP/OutboxWorker; a suíte Core inteira não foi executada. O conjunto final encerrou naturalmente com exit 0. Dois processos HTTP separados de fixture foram iniciados/encerrados no teste de replay, sem consumidor AMQP operacional.

## Fixtures finais de pesagem e migração

- Banco agrotrac_audit, schema novo **`audit_weight_it_1794778170531_e7c4cd37`**.
- Storage absoluto **`C:/Users/Luiz Fernando/orca/projects/Agrotrac/agrotrac-audit/tests/fixtures/generated/weighings_1794778170531_e7c4cd37`**.
- Antes de qualquer escrita foi verificado que o schema não tinha tabelas. O teste criou sua própria v1 por migrate(target=1), persistiu evento pelo caso de uso real Audit/PgLedger e somente depois aplicou v2 e reaplicou.
- Evento preservado: **`abca95d7-7519-4ad3-904f-4270692d66c0`**. Envelope, sequence e received_at foram comparados antes/depois sem regravação.
- Consulta SQL final confirmou **versions=[1,2], event_ledger=1 e weighings=9**.
- Schema próprio de histórico desconhecido: `audit_bad_history_1794778170531_e7c4cd37`, preservado em [1,99], sem weighings após rejeição.
- Schema próprio de lacuna: `audit_gap_history_1794778170531_e7c4cd37`, preservado com apenas audit_migrations e versão [2]; migração recusou sem adicionar tabelas.

Não foi aplicada migração a schema preexistente de outra tarefa. Todos os schemas da execução anterior do ledger/pool também foram criados novos pelo próprio teste. Não houve DROP/TRUNCATE/DELETE SQL, alteração de roles/senhas/credenciais ou mudanças na infra.

## Evidências dos cenários de pesagem

| Cenário | Resultado real assertado |
| --- | --- |
| 10 uploads concorrentes iguais | **1×201, 9×200, uma linha e um único arquivo final JPEG**; metade em ordem foto→JSON, metade JSON→foto; nome original ../../../../etc/passwd ignorado |
| Normalização | opcionais ausentes e null deram replay 200; captured_at sem milissegundos e com .000Z deram replay 200; ausência permaneceu null, sem preencher relógio do servidor |
| Resposta perdida após COMMIT | conexão HTTP destruída no onSend após resultado commitado, fetch falhou e SQL confirmou uma linha; replay após fechar/recriar servidor retornou 200 |
| Reinício de processo | além da nova instância, dois processos Node HTTP-only distintos leram o mesmo banco/storage e cada replay retornou 200, ainda uma linha; ambos encerrados com exit 0 |
| Conflito de dados e foto | mesmo local_id com placa diferente ou PNG diferente retornou 409; placa e foto JPEG originais permaneceram íntegros |
| JPEG e PNG reais | fotos geradas pelo encoder sharp a partir de pixels reais, JPEG **269088 bytes**, PNG **147998 bytes** na execução final, ambas aceitas e bytes originais preservados |
| Download | GET JPEG/PNG retornou SHA-256 exato, MIME/tamanho, ETag do hash, private,no-store e nosniff; UUID inválido 400, registro ausente 404 e traversal escapado 400/404 |
| Falha SQL antes COMMIT | exceção no hook após INSERT e arquivo final pronto resultou em 500, zero linhas e arquivo órfão presente; retry posterior verificou/reutilizou final e retornou 201 com uma linha |
| Falha de storage real | raiz configurada apontando para arquivo de fixture em vez de diretório retornou 500 STORAGE_IO_ERROR e zero linhas; multipart foi drenado e servidor fechou sem pendências |
| Pesos/payload inválidos | bruto zero, tara negativa/fracionária, inteiro acima do seguro, líquido divergente, bruto menor que tara, data impossível, booking_id inválido e campo desconhecido deram 400, sem promoção do PNG exclusivo dessas rejeições nem novas linhas |
| Campos multipart | parte extra, weighing duplicado, arquivo duplicado, foto/JSON ausentes, JSON com chave repetida e JSON >64KiB rejeitados antes da promoção/SQL; foto enviada antes do JSON também coberta |
| Precisão/constraints | Number.MAX_SAFE_INTEGER persistiu e foi consultado exato; UPDATE de tara negativa e de tamanho acima do teto SQL foram recusados por CHECK 23514, sem mudar a fixture |
| Integridade adicional | 8 casos rejeitados com 415: JPEG truncado, PNG truncado, falso cabeçalho JPEG, JPEGs concatenados, PNG com CRC corrompido, MIME divergente, texto renomeado e limite de pixels insuficiente |
| Limite inclusivo de bytes | foto com maxFileBytes igual ao tamanho foi aceita; max um byte abaixo retornou 413, sem linha |
| Multipart incompleto e upload interrompido | closing boundary truncado retornou 400; conexão foi destruída durante corpo ainda incompleto; zero linhas e nenhum final para o hash exclusivo de rejeição |
| Destino hash corrompido | arquivo novo controlado de fixture precriado no nome de hash correto com conteúdo inválido não foi substituído; upload deu 500 STORAGE_INTEGRITY_ERROR e zero linhas; conteúdo corrupto permaneceu idêntico |
| Junction/symlink | raiz de storage foi um junction real no Windows para diretório de fixture controlado; upload recusou com 500, zero linhas e destino externo da junction permaneceu vazio |
| Ordem de commit/cursor | primeiro INSERT ficou retido antes de COMMIT; segundo não pôde concluir nem ficou visível; liberado primeiro, ambos retornaram 201 e páginas limit=1 com mesmo booking_id retornaram os dois IDs sem perder commit tardio |
| Filtros/paginação | AND de placa e booking_id, cursor e limite cobertos; local_id sem resultados deu 200 items vazio; limite/cursor/placa/UUID/campo desconhecido inválidos deram 400 |

ID da resposta perdida na execução final: **`8697cde7-bf35-4d46-a50a-22eface77cc3`**. A falha SQL foi injetada na transação PostgreSQL real; não houve parada do banco. Testes de storage usaram FS real, hard link exclusivo e erros reais de caminho, não um substituto em memória. Unidade separada usa portas/adapters em memória e valida invariantes sem Docker.

## Arquivos, staging e durabilidade observada

Na raiz principal final: **3 arquivos finais** (JPEG, PNG e PNG verde), **49 arquivos em .staging** preservados. A asserção de deduplicação de 1 final ocorreu imediatamente após os 10 uploads concorrentes; depois outros cenários legítimos adicionaram imagens. Fotos inicialmente órfãs por conflito/falha SQL foram observadas e preservadas antes dos retries posteriores que puderam referenciá-las.

Inspeção recursiva final, sem seguir junctions, contou **59 arquivos e 1 junction** no diretório exclusivo, incluindo raízes auxiliares byte-limited, exact-byte-limit, pixel-limited, corruption e o arquivo que bloqueia criação de diretório. Todos foram preservados. A raiz auxiliar corruption contém deliberadamente um arquivo inválido, sem registro SQL correspondente; não é uma foto legítima confirmada. Não executar limpeza automática: retenção/expurgo exige autorização futura.

Promoção usa fs.link exclusivo, que teve sucesso real no volume Windows: concorrentes não substituem o destino. Há verificação de hash/tamanho/identidade antes de reutilizar final existente. Staging e final vencedor são hard links do mesmo inode; a aplicação nunca os reabre para escrita após validação. Não existe unlink, rm ou rename que substitua arquivo no código de storage. Alteração externa de qualquer alias pode corromper o conteúdo compartilhado; o próximo acesso detecta divergência e retorna erro, sem reparo automático.

Arquivo completo é fsynced antes de promoção; sync de diretórios foi tentado, mas o Windows/volume validado não o suportou e reportou **directoryFsyncSupported=false**. Isso está documentado, sem garantia absoluta contra queda de energia, cache de hardware, remoção externa ou corrupção. O storage requer árvore controlada pelo operador, sem substituição simultânea de componentes por usuários não confiáveis; validações lstat/realpath e O_NOFOLLOW onde disponível não substituem permissões de sistema.

## Regressão das entregas anteriores e recursos preservados

O ledger E2E anterior passou novamente com 8 eventos distintos, 35 ACKs do consumidor principal, 22 duplicados, 6 quarentenados, 2 retries SQL, 3 reconexões e lastError=null. Preservou seis mensagens na fila durável de rejeição e não consumiu filas operacionais.

- Schema Audit E2E: `audit_it_1794778170731_2d1e20bf`; schema Core exclusivo de fixtures: `core_audit_it_1794778170731_2d1e20bf`.
- Exchange e prefixo de filas: `agrotrac.audit.test.ab36e81e18cc4965b8e1603c31ba3f3e`, com sufixos `.input`, `.quarantine`, `.rejected`, `.live` e `.input.reconnect` conforme suíte anterior.
- Pool saturado: schema `audit_pool_it_1794778170168_5a62b393`; prefixo `agrotrac.audit.pool.5cc27cb729b34d50a4b713b1ece7ba68` e sufixos `.input`, `.quarantine`, `.rejected`.
- Pool min=max=1 com timeout 250ms: checkout em **265ms**, stop em **258ms**, zero ACK antes de persistência e uma reentrega posterior commitada/ACKada.

Os testes atuais criaram v2 nos próprios schemas novos dos testes de ledger/pool; nenhum schema antigo foi migrado. O código do consumidor e as interfaces do ledger foram preservados. Produção não importa Core; só o teste E2E com opt-in carrega o produtor real. Nenhum evento Audit de pesagem foi publicado.

## Histórico de implementação, sem ocultar falhas

Os runs iniciais expuseram dois problemas corrigidos: lstat no Windows reportou dev=0 enquanto fstat reportou ID real de volume, e um erro de storage anterior à leitura deixava a parte multipart sem drenagem. O primeiro foi corrigido com identidade de inode BigInt e comparação de device quando disponível; o segundo com drenagem antes do retorno. Outro run recebeu 500 para stream multipart incompleto; passou a distinguir erro do stream de falha de disco e retornar 400 sem promoção/SQL. Esses runs falhos/interrompidos não são declarados aprovação.

Diretórios/schemas de desenvolvimento preservados desta tarefa, identificados pelo sufixo: `1794777358623_a443b693`, `1794777436907_76210f0e`, `1794777486852_f1b37530`, `1794777514870_aba119e0`, `1794777603832_d467edd8` , `1794777832192_76667c08` e o final `1794778170531_e7c4cd37`. Schemas seguem audit_weight_it_/audit_bad_history_ e, nos três últimos runs, audit_gap_history_; diretórios seguem tests/fixtures/generated/weighings_. O primeiro diretório também preserva um hard link `.manual` criado somente no diagnóstico de FS; não é nome produzido pelo aplicativo. Os processos dos dois runs interrompidos foram encerrados; os runs finais concluíram naturalmente. Nenhum arquivo/fixture foi excluído.

## Limites e encerramento

- Testes comprovam os cenários executados; não são benchmark de carga, garantia absoluta de hardware ou resistência a administrador que altere o storage/banco.
- Validação verifica formato/estrutura/decodificação, não autenticidade documental nem conteúdo semântico da foto; não há OCR.
- Imagens limitadas em bytes, dimensões e pixels por upload; rate limiting global/autenticação continuam para integração futura. Limite de arquivo não pode superar CHECK SQL de 100MiB.
- Advisory lock curto por schema ordena commits para cursor estável e limita throughput; appends diretos externos fora do Core/Audit não são suportados. Role append-only de produção continua pendente, sem alteração de permissões nesta tarefa.
- Captured_at opcional não é prova do instante real; received_at é recepção SQL, não prova de commit. Não há política nova de descarga, tolerância, no-show, retenção ou retificação.
- Todos os servidores HTTP, processos HTTP filhos, conexões AMQP/SQL e proxies de teste foram fechados; inspeção de processos não encontrou weighings-server ou suíte de pesagens remanescentes. O relay autorizado foi parado ao concluir, sem excluir o container; PostgreSQL, Redis e RabbitMQ permaneceram healthy. Não houve alteração de .env/Compose/portas/volumes/credenciais.

# Contratos Canônicos de Ingestão de Pesagens e Romaneios — AgroTrac Audit

> **Documento Oficial de Engenharia e Arquitetura**  
> **Módulo:** `agrotrac-audit` (Serviço Audit C — Histórico, Auditoria, Ledger e Pesagens)  
> **Status:** Revisado pela coordenação; implementar com as correções vinculantes abaixo.  
> **Autor:** Arquiteto AgroTrac  
> **Data:** Novembro de 2026  
> **Destinatário Principal:** Desenvolvedor / Agente Codex (Audit C)

---

## Revisão vinculante da coordenação — 15/11/2026

Esta seção prevalece sobre exemplos conflitantes. Defaults operacionais não são regras definitivas de negócio.

- `captured_at` é opcional/nullable nesta API; o mobile deverá enviá-lo quando disponível. Não substituir ausência pelo relógio do servidor na comparação idempotente. Datas válidas UTC com ou sem milissegundos normalizam para o mesmo instante. Campos opcionais ausentes e null normalizam de forma estável; campos desconhecidos são rejeitados. `received_at` representa recepção/persistência no servidor, não prova do instante exato do commit.
- Fotos de romaneios podem conter dados pessoais. Responder `Cache-Control: private, no-store` e `X-Content-Type-Options: nosniff`, sem URL pública de storage ou caminho absoluto. Servidor permanece local até Gateway/autenticação. Não usar o cache público sugerido abaixo.
- `rename` sozinho não assegura ausência de sobrescrita em concorrência nem durabilidade contra queda de energia. Implementar promoção sem substituição do arquivo final (por exemplo link atômico exclusivo no mesmo volume, ou primitiva equivalente testada); se o destino existir, verificar hash/tamanho antes de reutilizar. Não apagar nem substituir arquivo final existente, inclusive em rollback ou conflito. Flush/fsync de arquivo e metadados quando suportado; documentar limites reais do SO/volume. Não alegar impossibilidade absoluta de referência quebrada após falha de hardware, remoção externa ou corrupção.
- Não excluir automaticamente arquivos, fixtures ou dados nesta etapa. Staging incompleto/órfão pode ser preservado e documentado; contagem de deduplicação refere-se aos arquivos finais. Retenção/limpeza posterior exige autorização. Diretório de storage é controlado pelo operador e não deve admitir symlinks/junctions inseridos por usuários não confiáveis; validar contenção real e rejeitar links nos componentes usados pelo storage, conforme suporte da plataforma.
- Assinatura inicial não prova imagem íntegra: rejeitar arquivos truncados e só com cabeçalho por validação estrutural adicional ou decodificação limitada. Impor limites de bytes e de dimensões/pixels se usar decoder, sem executar OCR nem aceitar formatos adicionais. MIME declarado deve ser coerente com o formato validado; nome original é ignorado. Testar JPEG e PNG reais e casos corrompidos, não apenas prefixos fabricados.
- Constraints SQL usam limites inteiros seguros e teto técnico de arquivo coerente com configuração validada. DDL ilustrativo abaixo não autoriza configurar acima do teto do banco. Não persistir caminhos arbitrários recebidos do cliente; preferir derivação interna por hash e formato.
- Concorrência sobre ID inexistente não é resolvida por SELECT FOR UPDATE sem linha. Usar UNIQUE(local_id) e INSERT ON CONFLICT com comparação após espera, ou advisory lock apropriado. Se usar sequence como cursor, garantir que appends concorrentes não tornem linhas commitadas depois invisíveis a clientes que já avançaram o cursor; seguir estratégia de serialização curta já usada no ledger ou equivalente comprovada.
- Migração deve aceitar explicitamente históricos [1] e [1,2], criar [1,2] em schema novo e recusar versões desconhecidas/lacunas sem alterar schema existente. Aplicar v2 a fixture v1 com evento real persistido e comprovar preservação; nenhuma regravação do ledger. Testes anteriores devem continuar passando.
- Upload parcial, campos/arquivos repetidos, partes desconhecidas ou tamanho excedido não podem deixar linha SQL nem confirmação de sucesso. Validar TODO multipart antes da promoção final/INSERT, independentemente da ordem das partes. GET listagem com filtro sem resultados retorna 200 com items vazio; distinguir local_id inválido (400), registro ausente no download (404) e falha técnica de storage (500) de forma documentada.

---

## 1. Contrato executável implementado

A revisão vinculante acima foi preservada integralmente. As seções seguintes substituem os exemplos anteriores conflitantes e descrevem `src/domain/weighing.ts`, `src/application/weighings.ts` e adapters separados de HTTP, PostgreSQL e storage.

Esta entrega adiciona pesagens/fotos ao Audit, preserva event_ledger e consumidor AMQP e não consulta Core, altera reserva/capacidade ou publica evento de produtor Audit. Sem OCR, nuvem ou políticas de descarga, tolerância de balança, retenção e retificação inventadas. Servidor segue em 127.0.0.1:3003 por padrão, sem autenticação pronta.

### POST /v1/audit/weighings/sync

Corpo multipart/form-data com EXATAMENTE duas partes, em qualquer ordem:

- `weighing`: campo textual JSON, com local_id, placa, peso_bruto_kg, tara_kg, peso_liquido_kg obrigatórios; booking_id, captured_at, motorista_nome, motorista_documento e observacoes opcionais/nullable.
- `romaneio`: exatamente um arquivo JPEG ou PNG íntegro, MIME declarado coerente; nome original ignorado, inclusive sequências de traversal.

UUID v4 canônico minúsculo para local_id/booking_id. Placa `^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$`. Massas são Number.isSafeInteger, bruto > 0, tara >= 0, bruto >= tara. Servidor calcula líquido = bruto - tara e exige igualdade do valor enviado, inclusive líquido zero válido. Campos desconhecidos e chaves JSON repetidas (inclusive grafia escapada equivalente) são rejeitados; não há coerção de números textuais. Texto opcional é preservado exatamente, até 100 caracteres para motorista_nome/documento e 500 para observacoes, sem validação fiscal. NUL/Unicode isolado incompatível com JSONB é rejeitado.

Opcionais ausentes e null normalizam para null. captured_at aceita UTC estrito com/sem milissegundos, valida calendário e normaliza ISO com milissegundos; ausência não vira relógio do servidor. received_at é clock_timestamp na inserção, não prova do instante exato do commit.

Valida o multipart COMPLETO antes da promoção final e do INSERT: parte extra/repetida/ausente, JSON inválido, limite excedido, erro de stream, boundary incompleto ou upload abortado não confirmam nem gravam linha SQL. Staging recebido pode permanecer. Falhas de storage anteriores à leitura drenam a parte para encerrar o parsing sem travar a requisição.

- 201: local_id novo, arquivo final verificado pronto antes do COMMIT.
- 200: retry do mesmo local_id com TODOS os campos normalizados e hash/tamanho/MIME iguais; retorna registro original sem alterar received_at/sequence.
- 409 WEIGHING_ID_CONFLICT: mesmo ID com dados ou foto diferentes; original preservado. A foto conflitante pode deixar órfão final, sem exclusão automática.

Resposta de sucesso: campos normalizados da pesagem, sequence como string, received_at UTC e `romaneio: {file_sha256,file_size_bytes,mime_type,url}`. URL é somente `/v1/audit/weighings/<local_id>/romaneio`, nunca path interno ou URL pública de storage.

### GET /v1/audit/weighings

Filtros opcionais exatos combinados por AND: placa, booking_id, local_id. `cursor` decimal textual não negativo até 9223372036854775807 (padrão 0); `limit` decimal textual 1–100 (padrão 50). Campos desconhecidos/inválidos retornam 400 INVALID_QUERY. Sem resultados: 200 `{items:[],next_cursor:null}`.

Ordena por sequence crescente, lê limit+1 e retorna `{items,next_cursor}`; next_cursor é a última sequence da página se existe próxima, senão null. Não converter cursor/sequence para Number. Advisory lock curto por schema serializa alocação da sequência/commit dos appends de pesagem, impedindo que um commit tardio de sequência menor seja perdido por quem avançou o cursor. Não há I/O de foto/AMQP dentro dessa transação. Paginação acompanha ingestão, não ordem de captured_at e não congela inserções futuras.

### GET /v1/audit/weighings/:local_id/romaneio

Valida UUID antes da consulta, deriva arquivo somente de hash e MIME persistidos e verifica arquivo inteiro/tamanho/SHA-256 antes de responder. 200 com bytes originais, MIME validado, Content-Length, Content-Disposition inline com nome interno `romaneio-<uuid>.jpg|png`, ETag com SHA-256, **Cache-Control private, no-store** e **X-Content-Type-Options nosniff**. POST e listagem também recebem esses cabeçalhos de privacidade. Não há servidor estático de uploads.

ID inválido: 400 INVALID_PAYLOAD. Registro ausente: 404 WEIGHING_NOT_FOUND. Arquivo ausente, diretório inseguro ou falha de acesso: 500 STORAGE_IO_ERROR; arquivo encontrado com hash/tamanho/identidade divergente: 500 STORAGE_INTEGRITY_ERROR. Não tenta restaurar/substituir dados automaticamente. O GET protege os bytes em trânsito HTTP local; segurança externa depende de Gateway/autenticação futura.

## 2. Storage e validação de imagem

`DiskPhotos` exige raiz absoluta. Configuração padrão resolve uploads/romaneios sob cwd para caminho absoluto; AUDIT_STORAGE_PATH fornecido deve ser absoluto. O operador controla toda a árvore e não deve permitir troca concorrente de componentes por usuários não confiáveis.

Antes de usar caminhos, percorre componentes, valida lstat/realpath, rejeita symlink/junction/diretório indireto; nomes internos são UUID de staging e SHA-256 hexadecimal, nunca nomes do cliente. Abertura usa O_NOFOLLOW onde suportado, verifica tipo/tamanho e identidade de inode com BigInt entre lstat/fstat; no Windows lstat pode informar dev=0 enquanto fstat informa volume real, por isso compara dev quando disponível. Essas verificações não são garantia contra alteração simultânea por administrador/usuário com acesso de escrita ao storage.

Pipeline:

1. Cria `.staging/<uuid>` exclusivamente, escreve chunks e calcula SHA-256/tamanho em streaming com limite de bytes, sem publicar caminho.
2. Faz fsync do arquivo e tenta fsync do diretório staging quando suportado. Lê no máximo o arquivo limitado para validar estrutura e decodificar.
3. JPEG exige estrutura de segmentos/entropy markers e EOI final, sem JPEG concatenado/trailing bytes. PNG exige IHDR, IDAT, IEND final e CRC correto de TODOS os chunks; APNG é recusado.
4. Sharp/libvips com failOn warning força decodificação RAW completa, limite padrão de 16777216 pixels, largura/altura máximas 8192, uma página/frame. Metadados/assinatura sozinhos não bastam. Não executa OCR nem converte/regrava a imagem original.
5. Só depois de validar todo multipart/domínio, verifica staging novamente e promove por **hard link exclusivo** no mesmo volume para `<sha[0:2]>/<sha>.jpg|png`. Link existente não é sobrescrito: verifica tamanho/hash e identidade do destino; se corrupto, retorna erro e preserva seu conteúdo.
6. Tenta fsync do diretório do hash e raiz. Somente então abre a transação SQL. Falha SQL pode deixar final órfão, que permanece para retry e inspeção.

Nenhum staging/final é apagado, nem em rollback, conflito ou retry. Cada retry pode preservar um staging adicional; deduplicação é de nomes finais por hash/formato, não contagem de staging. Hard link deixa staging completo e final apontando ao mesmo inode; a aplicação nunca escreve nesses arquivos depois da validação/promoção. Alteração externa de qualquer alias pode corromper o final e será detectada no próximo acesso. Retenção/limpeza/hardening adicional do operador requer autorização própria.

Arquivo foi fsynced nos testes. No Windows validado, fsync de diretório não foi suportado (`directoryFsyncSupported=false`), reportado no teste; erros conhecidos de suporte/abertura de diretório são tolerados, outras falhas são erro de storage. Não há garantia absoluta contra falha de hardware, cache do volume/controladora, remoção externa ou corrupção. Falha posterior pode exigir recuperação operacional; a aplicação não promete impossibilidade de referência quebrada.

## 3. Limites técnicos configuráveis

| Configuração | Default | Teto validado |
| --- | --- | --- |
| AUDIT_STORAGE_PATH | caminho absoluto resolvido de uploads/romaneios | deve ser absoluto, sem links nos componentes usados |
| AUDIT_MAX_FILE_BYTES | 10485760 (10 MiB) | inteiro positivo até 104857600 (100 MiB), também teto CHECK SQL |
| AUDIT_MAX_JSON_BYTES | 65536 (64 KiB) | inteiro positivo até 65536 |
| AUDIT_MAX_IMAGE_PIXELS | 16777216 | inteiro positivo até 16777216; e largura/altura <=8192 |

A validação mantém limite de buffers e decoder por upload; não é rate limiting global nem benchmark de concorrência. Configuração acima do teto do banco é recusada antes de abrir pool/servidor. Exatamente o limite de bytes é aceito; acima retorna erro. As margens internas de +1 byte no parser permitem distinguir o limite inclusivo e o excesso; verificação própria aplica o valor configurado.

## 4. PostgreSQL e migração aditiva v2

Migração usa advisory lock de esquema e aceita somente históricos ordenados [1] e [1,2], sem tabelas desconhecidas. Schema novo recebe v1 e v2 na mesma transação; v1 reconhecido recebe só a nova tabela/índices/registro de v2; reaplicação [1,2] não regrava ledger. Histórico desconhecido/lacuna aborta antes de alteração. Argumento target=1 existe para criar fixture própria v1 em teste, não downgrade de schema v2. Nenhuma fixture preexistente de outra entrega é migrada nos testes.

`weighings`: local_id UUID PRIMARY KEY, sequence BIGSERIAL UNIQUE, placa, booking_id nullable sem FK para Core, captured_at nullable, bruto/tara/líquido BIGINT com limites seguros e igualdade matemática, data JSONB normalizado coerente com colunas, SHA-256, tamanho com CHECK até 100MiB, MIME JPEG/PNG e received_at. Não armazena filepath arbitrário. Índices de placa/booking_id por sequence e de hash. Appends são INSERT ON CONFLICT DO NOTHING e comparação do registro após a espera pelo vencedor; conflito não faz UPDATE. A transação só recebe metadados de arquivo já promovido/verificado.

Pesagens são append-only na aplicação. Não há alteração de roles/permissões; role de produção restrita e política de retificação continuam pendentes. Não modifica reserva/capacidade nem emite novo evento Audit.

## 5. Erros e evidências

Erros usam o envelope AgroTrac application/json `{error:{code,message,details:null,correlation_id,timestamp}}`, sem paths/stack/segredos, mesmo header de correlação do ledger. Campos message usam o código sanitizado nesta implementação.

| Código | HTTP |
| --- | --- |
| INVALID_MULTIPART_REQUEST, UNEXPECTED_FIELD, UNEXPECTED_FILE_COUNT | 400 |
| INVALID_JSON_PAYLOAD, JSON_PAYLOAD_TOO_LARGE, INVALID_PAYLOAD, INVALID_NET_WEIGHT | 400 |
| INVALID_QUERY, INVALID_CORRELATION_ID | 400 |
| FILE_TOO_LARGE | 413 |
| INVALID_FILE_SIGNATURE (inclui estrutura, corrupção, MIME e limites decoder) | 415 |
| WEIGHING_NOT_FOUND | 404 |
| WEIGHING_ID_CONFLICT | 409 |
| STORAGE_IO_ERROR, STORAGE_INTEGRITY_ERROR, INTERNAL_SERVER_ERROR | 500 |

Resultado real e recursos preservados estão em **docs/weighings-validation.md**. Histórico anterior do ledger em docs/validation.md foi preservado. Unidade usa adapters em memória, sem Docker; integração usa HTTP multipart, PostgreSQL e FS reais, cria v1/evento antes da migração v2 e executa também as suítes Audit anteriores, sem repetir a suíte Core inteira.
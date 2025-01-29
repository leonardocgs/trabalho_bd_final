import duckdb
import os
import matplotlib.pyplot as plt

DDL = r"""
CREATE SEQUENCE seq_usuario;
CREATE TABLE tbl_usuario (
    id_usuario INTEGER DEFAULT nextval('seq_usuario') PRIMARY KEY,
    nm_usuario VARCHAR(200) NOT NULL,
    cpf_usuario CHAR(11) NOT NULL UNIQUE,
    email_usuario VARCHAR(200) NOT NULL UNIQUE,
    senha_usuario VARCHAR(255) NOT NULL,
    dt_criacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE seq_empresa;
CREATE TABLE tbl_empresa (
    id_empresa INTEGER DEFAULT nextval('seq_empresa') PRIMARY KEY,
    nm_fantasia_empresa VARCHAR(60) NOT NULL,
    razao_social_empresa VARCHAR(100) NOT NULL,
    cnpj_empresa CHAR(14) NOT NULL UNIQUE,
    rua_empresa VARCHAR(100) NOT NULL,
    numero_empresa VARCHAR(10) NOT NULL,
    bairro_empresa VARCHAR(50) NOT NULL,
    cidade_empresa VARCHAR(50) NOT NULL,
    estado_empresa CHAR(2) NOT NULL,
    cep_empresa CHAR(8) NOT NULL,
    dt_cadastro_empresa TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_estado_empresa CHECK (estado_empresa ~ '^[A-Z]{2}$')
);

CREATE SEQUENCE seq_senioridade;
CREATE TABLE tbl_senioridade (
    id_senioridade INTEGER DEFAULT nextval('seq_senioridade') PRIMARY KEY,
    ds_senioridade VARCHAR(20) NOT NULL
);

CREATE SEQUENCE seq_especialidade;
CREATE TABLE tbl_cargo_especialidade (
    id_cargo_especialidade INTEGER DEFAULT nextval('seq_especialidade') PRIMARY KEY,
    ds_cargo_especialidade VARCHAR(60) NOT NULL
);

CREATE SEQUENCE tbl_vinculo_usuario_empresa;
CREATE TABLE tbl_vinculo_usuario_empresa (
    id_vinculo INTEGER DEFAULT nextval('tbl_vinculo_usuario_empresa') PRIMARY KEY, id_usuario INTEGER NOT NULL,
    id_empresa INTEGER NOT NULL,
    id_cargo_especialidade INTEGER NOT NULL,
    id_senioridade INTEGER NOT NULL,
    salario_vinculo DECIMAL(10,2) NOT NULL,
    cod_regime_contratacao INTEGER NOT NULL,
    cod_modelo_trabalho INTEGER NOT NULL,
    carga_horaria_vinculo INTEGER NOT NULL,
    cod_turno INTEGER NOT NULL,
    dt_inicio_vinculo TIMESTAMP NOT NULL,
    emprego_atual BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (id_usuario) REFERENCES tbl_usuario (id_usuario),
    FOREIGN KEY (id_empresa) REFERENCES tbl_empresa (id_empresa),
    FOREIGN KEY (id_cargo_especialidade) REFERENCES tbl_cargo_especialidade (id_cargo_especialidade),
    FOREIGN KEY (id_senioridade) REFERENCES tbl_senioridade (id_senioridade),
    CONSTRAINT chk_regime_contratacao CHECK (cod_regime_contratacao IN (1, 2)),
    CONSTRAINT chk_modelo_trabalho CHECK (cod_modelo_trabalho IN (1, 2, 3)),
    CONSTRAINT chk_turno CHECK (cod_turno IN (1, 2, 3)),
    CONSTRAINT chk_carga_horaria CHECK (carga_horaria_vinculo > 0 AND carga_horaria_vinculo <= 44)
);

CREATE SEQUENCE seq_tbl_avaliacao;
CREATE TABLE tbl_avaliacao (
    id_avaliacao INTEGER DEFAULT nextval('seq_tbl_avaliacao') PRIMARY KEY,
    id_usuario INTEGER NOT NULL,
    id_empresa INTEGER NOT NULL,
    cargo_mais_requisitado INTEGER,
    faz_hora_extra BOOLEAN NOT NULL,
    hora_extra_remunerada BOOLEAN,
    tempo_primeira_promocao INTEGER,
    percent_promocao DECIMAL(5,2),
    tempo_primeiro_aumento INTEGER,
    percent_aumento DECIMAL(5,2),
    cod_problema_pj INTEGER,
    cod_assedio INTEGER,
    depoimento_geral TEXT NOT NULL,
    nota_geral INTEGER NOT NULL,
    dt_avaliacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_usuario) REFERENCES tbl_usuario (id_usuario),
    FOREIGN KEY (id_empresa) REFERENCES tbl_empresa (id_empresa),
    FOREIGN KEY (cargo_mais_requisitado) REFERENCES tbl_cargo_especialidade (id_cargo_especialidade),
    CONSTRAINT chk_nota CHECK (nota_geral BETWEEN 1 AND 5),
    CONSTRAINT chk_problema_pj CHECK (cod_problema_pj IN (1, 2, 3, 4)),
    CONSTRAINT chk_assedio CHECK (cod_assedio IN (1, 2)),
    CONSTRAINT chk_hora_extra_remunerada CHECK (
        (faz_hora_extra = false AND hora_extra_remunerada IS NULL) OR
        (faz_hora_extra = true AND hora_extra_remunerada IS NOT NULL)
    )
);

CREATE SEQUENCE seq_tbl_beneficio;
CREATE TABLE tbl_beneficio (
    id_beneficio INTEGER DEFAULT nextval('seq_tbl_beneficio')  PRIMARY KEY,
    ds_beneficio VARCHAR(60) NOT NULL UNIQUE
);

CREATE SEQUENCE seq_tbl_avaliacao_beneficio;
CREATE TABLE tbl_avaliacao_beneficio (
    id_avaliacao_beneficio INTEGER DEFAULT nextval('seq_tbl_avaliacao_beneficio') PRIMARY KEY,
    id_avaliacao INTEGER NOT NULL,
    id_beneficio INTEGER NOT NULL,
    FOREIGN KEY (id_avaliacao) REFERENCES tbl_avaliacao (id_avaliacao),
    FOREIGN KEY (id_beneficio) REFERENCES tbl_beneficio (id_beneficio),
    UNIQUE (id_avaliacao, id_beneficio)
);

"""
POPULATE = r"""
INSERT INTO tbl_senioridade (ds_senioridade) VALUES
('Júnior'),     -- id_senioridade = 1
('Pleno'),      -- id_senioridade = 2
('Sênior');     -- id_senioridade = 3

-- 3.2. CARGOS
INSERT INTO tbl_cargo_especialidade (ds_cargo_especialidade) VALUES
('Desenvolvedor'),       -- id_cargo_especialidade = 1
('Analista'),            -- 2
('Tech Lead'),           -- 3
('QA'),                  -- 4
('DevOps'),              -- 5
('Arquiteto de Software'); -- 6

-- 3.3. BENEFÍCIOS
INSERT INTO tbl_beneficio (ds_beneficio) VALUES
('Vale-Refeição'),       -- id_beneficio = 1
('Vale-Transporte'),     -- 2
('Plano de Saúde'),      -- 3
('Seguro de Vida'),      -- 4
('Auxílio Creche'),      -- 5
('Participação nos Lucros e Resultados (PLR)'),  -- 6
('Treinamento Pago'),    -- 7
('Academia'),            -- 8
('Cartão Alimentação');  -- 9

------------------------------------------------------------------------------
-- 4. INSERÇÃO DE USUÁRIOS (15 USUÁRIOS)
------------------------------------------------------------------------------

INSERT INTO tbl_usuario (nm_usuario, cpf_usuario, email_usuario, senha_usuario) VALUES
('Ana Silva',        '11111111111', 'ana.silva@example.com',        'senha111'),
('Bruno Souza',      '22222222222', 'bruno.souza@example.com',      'senha222'),
('Carla Mendes',     '33333333333', 'carla.mendes@example.com',     'senha333'),
('Daniel Oliveira',  '44444444444', 'daniel.oliveira@example.com',  'senha444'),
('Eva Pereira',      '55555555555', 'eva.pereira@example.com',      'senha555'),
('Felipe Costa',     '66666666666', 'felipe.costa@example.com',     'senha666'),
('Gabriela Lima',    '77777777777', 'gabriela.lima@example.com',    'senha777'),
('Henrique Rocha',   '88888888888', 'henrique.rocha@example.com',   'senha888'),
('Isabela Martins',  '99999999999', 'isabela.martins@example.com',  'senha999'),
('João Paulo',       '11122233344', 'joao.paulo@example.com',       'senhaabc'),
('Karina Alves',     '22233344455', 'karina.alves@example.com',     'senhaabcd'),
('Leonardo Dias',    '33344455566', 'leonardo.dias@example.com',    'senha1234'),
('Mariana Barbosa',  '44455566677', 'mariana.barbosa@example.com',  'senha5678'),
('Nathan Ferreira',  '55566677788', 'nathan.ferreira@example.com',  'senha9876'),
('Olivia Gomes',     '66677788899', 'olivia.gomes@example.com',     'senha6543');

------------------------------------------------------------------------------
-- 5. INSERÇÃO DE EMPRESAS (5 EMPRESAS)
------------------------------------------------------------------------------

INSERT INTO tbl_empresa (
    nm_fantasia_empresa, 
    razao_social_empresa, 
    cnpj_empresa, 
    rua_empresa, 
    numero_empresa, 
    bairro_empresa, 
    cidade_empresa, 
    estado_empresa, 
    cep_empresa
) VALUES
('Tech Innovators', 'Tech Innovators Ltda.',     '10000000000001', 'Av. Paulista',  '1000', 'Bela Vista', 'São Paulo',    'SP', '01311000'),
('Data Solutions',  'Data Solutions S.A.',       '20000000000002', 'Rua das Flores','500',  'Centro',     'Rio de Janeiro','RJ', '20090000'),
('Cloud Services',  'Cloud Services ME',         '30000000000003', 'Av. Rio Branco','200',  'Centro',     'Rio de Janeiro','RJ', '20090001'),
('GreenTech',       'GreenTech Ltda.',           '40000000000004', 'Rua Acácias',   '750',  'Botafogo',   'Rio de Janeiro','RJ', '22270000'),
('Alpha Systems',   'Alpha Systems S.A.',        '50000000000005', 'Av. Atlântica', '3000', 'Copacabana', 'Rio de Janeiro','RJ', '22021000');

------------------------------------------------------------------------------
-- 6. INSERÇÃO DE VÍNCULOS (2 POR USUÁRIO = 30 INSERÇÕES)
------------------------------------------------------------------------------

-- Regras:
--  - cod_regime_contratacao: (1=CLT, 2=PJ)
--  - cod_modelo_trabalho: (1=Presencial, 2=Híbrido, 3=Remoto)
--  - cod_turno: (1=Diurno, 2=Noturno, 3=Outro)
--  - Apenas 1 vínculo atual (emprego_atual=TRUE) por usuário

-- Vincularemos cada usuário a 2 empresas diferentes.

------------------------------------------------------------------------------
-- 6. INSERÇÃO DE VÍNCULOS
------------------------------------------------------------------------------
-- Regras:
--  - cod_regime_contratacao: (1=CLT, 2=PJ)
--  - cod_modelo_trabalho: (1=Presencial, 2=Híbrido, 3=Remoto)
--  - cod_turno: (1=Diurno, 2=Noturno, 3=Outro)
--  - Apenas 1 vínculo atual (emprego_atual=TRUE) por usuário (nos vínculos originais).
--  - Aqui adicionamos mais 1 vínculo PJ para cada usuário (emprego_atual=FALSE).

-- -------------------------
-- Vínculos Originais (30)
-- -------------------------
INSERT INTO tbl_vinculo_usuario_empresa (
    id_usuario,
    id_empresa,
    id_cargo_especialidade,
    id_senioridade,
    salario_vinculo,
    cod_regime_contratacao,
    cod_modelo_trabalho,
    carga_horaria_vinculo,
    cod_turno,
    dt_inicio_vinculo,
    emprego_atual
) VALUES

-- Usuario 1 -> Empresa 1 (atual), Empresa 2 (passado)
(1, 1, 1, 1, 3000.00, 1, 1, 40, 1, '2022-01-10', TRUE),
(1, 2, 2, 1, 3100.00, 1, 2, 40, 1, '2021-01-10', FALSE),

-- Usuario 2 -> Empresa 1 (passado), Empresa 2 (atual)
(2, 1, 3, 2, 4000.00, 2, 2, 40, 2, '2020-05-01', FALSE),
(2, 2, 1, 2, 4200.00, 2, 1, 40, 2, '2021-05-01', TRUE),

-- Usuario 3 -> Empresa 2 (passado), Empresa 3 (atual)
(3, 2, 2, 3, 4500.00, 1, 3, 40, 1, '2020-03-15', FALSE),
(3, 3, 3, 3, 4600.00, 1, 2, 40, 1, '2021-03-15', TRUE),

-- Usuario 4 -> Empresa 3 (passado), Empresa 4 (atual)
(4, 3, 1, 1, 3200.00, 2, 1, 40, 2, '2022-02-20', FALSE),
(4, 4, 2, 1, 3300.00, 2, 3, 40, 2, '2023-02-20', TRUE),

-- Usuario 5 -> Empresa 4 (passado), Empresa 5 (atual)
(5, 4, 3, 2, 3800.00, 1, 2, 40, 1, '2022-01-01', FALSE),
(5, 5, 1, 2, 3900.00, 1, 1, 40, 1, '2023-01-01', TRUE),

-- Usuario 6 -> Empresa 1 (atual), Empresa 3 (passado)
(6, 1, 2, 3, 5000.00, 2, 1, 40, 2, '2021-07-01', TRUE),
(6, 3, 3, 3, 5200.00, 2, 3, 40, 2, '2020-07-01', FALSE),

-- Usuario 7 -> Empresa 2 (atual), Empresa 4 (passado)
(7, 2, 1, 1, 2800.00, 1, 2, 40, 1, '2023-03-10', TRUE),
(7, 4, 5, 1, 2900.00, 1, 1, 40, 1, '2022-03-10', FALSE),

-- Usuario 8 -> Empresa 3 (atual), Empresa 5 (passado)
(8, 3, 6, 2, 6000.00, 2, 3, 40, 2, '2023-06-05', TRUE),
(8, 5, 4, 2, 6100.00, 2, 2, 40, 2, '2022-06-05', FALSE),

-- Usuario 9 -> Empresa 1 (passado), Empresa 4 (atual)
(9, 1, 1, 3, 7000.00, 1, 2, 40, 1, '2021-08-12', FALSE),
(9, 4, 3, 3, 7300.00, 1, 1, 40, 1, '2022-08-12', TRUE),

-- Usuario 10 -> Empresa 2 (passado), Empresa 5 (atual)
(10, 2, 2, 1, 3500.00, 2, 2, 40, 2, '2020-10-01', FALSE),
(10, 5, 6, 1, 3800.00, 2, 1, 40, 2, '2021-10-01', TRUE),

-- Usuario 11 -> Empresa 3 (atual), Empresa 4 (passado)
(11, 3, 1, 2, 4100.00, 1, 3, 40, 1, '2022-04-01', TRUE),
(11, 4, 2, 2, 3900.00, 1, 2, 40, 1, '2021-04-01', FALSE),

-- Usuario 12 -> Empresa 1 (atual), Empresa 2 (passado)
(12, 1, 3, 3, 5500.00, 2, 1, 40, 2, '2022-09-10', TRUE),
(12, 2, 4, 3, 5600.00, 2, 2, 40, 2, '2021-09-10', FALSE),

-- Usuario 13 -> Empresa 4 (atual), Empresa 5 (passado)
(13, 4, 1, 1, 3200.00, 1, 3, 40, 1, '2023-11-01', TRUE),
(13, 5, 2, 1, 3300.00, 1, 1, 40, 1, '2022-11-01', FALSE),

-- Usuario 14 -> Empresa 3 (atual), Empresa 5 (passado)
(14, 3, 3, 2, 4700.00, 2, 3, 40, 2, '2023-02-15', TRUE),
(14, 5, 4, 2, 4800.00, 2, 2, 40, 2, '2022-02-15', FALSE),

-- Usuario 15 -> Empresa 1 (atual), Empresa 2 (passado)
(15, 1, 6, 3, 7500.00, 1, 1, 40, 1, '2023-12-20', TRUE),
(15, 2, 5, 3, 7600.00, 1, 2, 40, 1, '2022-12-20', FALSE);

-- ------------------------------------------------
-- NOVOS VÍNCULOS (1 extra por usuário, todos PJ)
-- ------------------------------------------------
INSERT INTO tbl_vinculo_usuario_empresa (
    id_usuario, id_empresa,
    id_cargo_especialidade, id_senioridade,
    salario_vinculo, cod_regime_contratacao,
    cod_modelo_trabalho, carga_horaria_vinculo,
    cod_turno, dt_inicio_vinculo,
    emprego_atual
)
VALUES
-- Usuario 1 (PJ) - Empresa 3
(1, 3, 1, 1, 2700.00, 2, 3, 35, 3, '2020-01-05', FALSE),
-- Usuario 2 (PJ) - Empresa 5
(2, 5, 1, 2, 4000.00, 2, 2, 40, 1, '2019-12-10', FALSE),
-- Usuario 3 (PJ) - Empresa 4
(3, 4, 2, 1, 3150.00, 2, 1, 40, 2, '2021-05-20', FALSE),
-- Usuario 4 (PJ) - Empresa 2
(4, 2, 3, 2, 4200.00, 2, 2, 40, 2, '2020-11-15', FALSE),
-- Usuario 5 (PJ) - Empresa 3
(5, 3, 1, 3, 6100.00, 2, 1, 40, 2, '2021-02-10', FALSE),
-- Usuario 6 (PJ) - Empresa 5
(6, 5, 2, 2, 3600.00, 2, 3, 40, 1, '2020-09-01', FALSE),
-- Usuario 7 (PJ) - Empresa 1
(7, 1, 5, 2, 2900.00, 2, 2, 40, 1, '2020-10-10', FALSE),
-- Usuario 8 (PJ) - Empresa 4
(8, 4, 6, 1, 3100.00, 2, 1, 40, 1, '2022-01-05', FALSE),
-- Usuario 9 (PJ) - Empresa 5
(9, 5, 3, 2, 3500.00, 2, 3, 35, 2, '2021-07-07', FALSE),
-- Usuario 10 (PJ) - Empresa 1
(10, 1, 2, 3, 5300.00, 2, 2, 40, 2, '2020-10-22', FALSE),
-- Usuario 11 (PJ) - Empresa 2
(11, 2, 2, 2, 4050.00, 2, 1, 40, 1, '2021-06-06', FALSE),
-- Usuario 12 (PJ) - Empresa 4
(12, 4, 3, 1, 2800.00, 2, 2, 35, 3, '2022-02-02', FALSE),
-- Usuario 13 (PJ) - Empresa 2
(13, 2, 1, 2, 3750.00, 2, 3, 40, 1, '2021-10-20', FALSE),
-- Usuario 14 (PJ) - Empresa 1
(14, 1, 6, 3, 6950.00, 2, 2, 40, 2, '2022-04-04', FALSE),
-- Usuario 15 (PJ) - Empresa 5
(15, 5, 5, 3, 4800.00, 2, 1, 40, 2, '2021-11-11', FALSE);
------------------------------------------------------------------------------
-- 7. INSERÇÃO DE AVALIAÇÕES (2 POR USUÁRIO = 30 INSERÇÕES)
--
-- Regras:
--  - Cada avaliação deve corresponder a um vínculo existente entre (id_usuario, id_empresa).
--  - Se faz_hora_extra = FALSE => hora_extra_remunerada = NULL
--  - Se faz_hora_extra = TRUE => hora_extra_remunerada = TRUE ou FALSE (mas não NULL)
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- 7. AVALIAÇÕES ORIGINAIS (30)
------------------------------------------------------------------------------
INSERT INTO tbl_avaliacao (
    id_usuario,
    id_empresa,
    cargo_mais_requisitado,
    faz_hora_extra,
    hora_extra_remunerada,
    tempo_primeira_promocao,
    percent_promocao,
    tempo_primeiro_aumento,
    percent_aumento,
    cod_problema_pj,
    cod_assedio,
    depoimento_geral,
    nota_geral
) VALUES

-- Usuario 1, Empresa 1 (FAZ hora extra)
(1, 1, 1, TRUE, TRUE, 12, 10.00, 6, 5.00, NULL, NULL, 'Ambiente excelente, grande aprendizado.', 5),
-- Usuario 1, Empresa 2 (NÃO FAZ hora extra)
(1, 2, 2, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Projeto interessante, mas poderia melhorar.', 4),

-- Usuario 2, Empresa 1
(2, 1, 3, FALSE, NULL, NULL, NULL, NULL, NULL, 2, 1, 'Tive problemas com PJ, mas liderança era boa.', 3),
-- Usuario 2, Empresa 2
(2, 2, 1, TRUE, TRUE, 8, 12.00, 4, 6.00, NULL, NULL, 'Boas oportunidades de crescimento.', 4),

-- Usuario 3, Empresa 2
(3, 2, 2, TRUE, TRUE, 10, 10.00, 8, 8.00, NULL, 2, 'Trabalho desafiador, mas horas extras existem.', 4),
-- Usuario 3, Empresa 3
(3, 3, 3, TRUE, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, 'Ambiente equilibrado e suporte adequado.', 5),

-- Usuario 4, Empresa 3
(4, 3, 1, TRUE, FALSE, 18, 15.00, 12, 10.00, NULL, NULL, 'Fazemos hora extra, mas nem sempre é paga.', 3),
-- Usuario 4, Empresa 4
(4, 4, 2, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Empresa em crescimento, equipe unida.', 4),

-- Usuario 5, Empresa 4
(5, 4, 3, TRUE, TRUE, 6, 10.00, 3, 5.00, NULL, NULL, 'Horário flexível e política de benefícios boa.', 5),
-- Usuario 5, Empresa 5
(5, 5, 1, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Remuneração poderia ser melhor, mas ambiente ótimo.', 4),

-- Usuario 6, Empresa 1
(6, 1, 2, TRUE, TRUE, 12, 12.00, 6, 6.00, NULL, NULL, 'Empresa inovadora, mas gestão de pessoas precisa evoluir.', 4),
-- Usuario 6, Empresa 3
(6, 3, 3, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Projetos interessantes e desafiadores.', 5),

-- Usuario 7, Empresa 2
(7, 2, 1, TRUE, FALSE, 10, 8.00, 4, 4.00, NULL, NULL, 'Equilíbrio razoável entre vida pessoal e trabalho.', 4),
-- Usuario 7, Empresa 4
(7, 4, 5, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Precisa melhorar a comunicação interna.', 3),

-- Usuario 8, Empresa 3
(8, 3, 6, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Ambiente remoto, mas sem muita integração de equipe.', 4),
-- Usuario 8, Empresa 5
(8, 5, 4, TRUE, TRUE, 9, 10.00, 5, 5.00, NULL, NULL, 'Bons líderes, metodologia ágil bem aplicada.', 5),

-- Usuario 9, Empresa 1
(9, 1, 1, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Gestão antiga, mas em processo de modernização.', 4),
-- Usuario 9, Empresa 4
(9, 4, 3, TRUE, TRUE, 20, 15.00, 10, 8.00, NULL, NULL, 'Benefícios excelentes, equipe madura tecnicamente.', 5),

-- Usuario 10, Empresa 2
(10, 2, 2, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Muito trabalho repetitivo, pouca inovação.', 3),
-- Usuario 10, Empresa 5
(10, 5, 6, TRUE, TRUE, 12, 10.00, 6, 4.00, NULL, NULL, 'Boa remuneração e liberdade de escolha de projetos.', 5),

-- Usuario 11, Empresa 3
(11, 3, 1, TRUE, FALSE, 18, 10.00, 9, 7.00, NULL, NULL, 'Empresa com mindset ágil e foco em resultados.', 4),
-- Usuario 11, Empresa 4
(11, 4, 2, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Várias oportunidades de promoção interna.', 3),

-- Usuario 12, Empresa 1
(12, 1, 3, TRUE, TRUE, 7, 8.00, 3, 3.00, NULL, 2, 'Boa cultura de colaboração, mas metas agressivas.', 4),
-- Usuario 12, Empresa 2
(12, 2, 4, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Processos um pouco burocráticos.', 3),

-- Usuario 13, Empresa 4
(13, 4, 1, TRUE, TRUE, 5, 5.00, 2, 2.00, NULL, NULL, 'Benefícios médios, mas projetos interessantes.', 4),
-- Usuario 13, Empresa 5
(13, 5, 2, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Ótimo ambiente, mas crescimento lento na carreira.', 3),

-- Usuario 14, Empresa 3
(14, 3, 3, TRUE, TRUE, 10, 12.00, 5, 4.00, NULL, NULL, 'Ótimos gestores, cultura bem definida.', 5),
-- Usuario 14, Empresa 5
(14, 5, 4, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Falta de transparência em algumas decisões.', 3),

-- Usuario 15, Empresa 1
(15, 1, 6, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Empresa grande, processo mais lento de mudança.', 3),
-- Usuario 15, Empresa 2
(15, 2, 5, TRUE, TRUE, 15, 15.00, 7, 7.00, NULL, NULL, 'Horas extras são remuneradas, clima descontraído.', 5);

------------------------------------------------------------------------------
-- NOVAS AVALIAÇÕES (15) PARA OS VÍNCULOS PJ ADICIONAIS
------------------------------------------------------------------------------
INSERT INTO tbl_avaliacao (
    id_usuario, id_empresa,
    cargo_mais_requisitado,
    faz_hora_extra, hora_extra_remunerada,
    tempo_primeira_promocao, percent_promocao,
    tempo_primeiro_aumento, percent_aumento,
    cod_problema_pj,      -- (1..4)
    cod_assedio,          -- ou NULL
    depoimento_geral,
    nota_geral
) VALUES
-- User 1, Emp 3 (PJ) => cod_problema_pj = 1 (Irregularidades contratuais)
(1, 3, 1, TRUE, FALSE, 10, 10.00, 5, 5.00, 1, NULL,
 'Contratado como PJ, mas sem contrato claro de hora extra.', 3),

-- User 2, Emp 5 (PJ) => cod_problema_pj = 2 (CLT mascarado de PJ)
(2, 5, 1, FALSE, NULL, NULL, NULL, NULL, NULL, 2, NULL,
 'Relação de trabalho exigia horário fixo, mas era PJ.', 3),

-- User 3, Emp 4 (PJ) => cod_problema_pj = 3 (Não pagamento de férias/13°)
(3, 4, 2, TRUE, TRUE, 8, 9.00, 4, 4.00, 3, NULL,
 'Tudo estava ótimo até notar falta de 13° para PJ.', 3),

-- User 4, Emp 2 (PJ) => cod_problema_pj = 2
(4, 2, 3, TRUE, FALSE, 12, 10.00, 7, 5.00, 2, NULL,
 'Cobra presença diária como CLT, mas é PJ.', 2),

-- User 5, Emp 3 (PJ) => cod_problema_pj = 4 (Falta de garantia de direitos trabalhistas)
(5, 3, 1, FALSE, NULL, NULL, NULL, NULL, NULL, 4, NULL,
 'Não há nenhuma garantia trabalhista no contrato PJ.', 3),

-- User 6, Emp 5 (PJ) => cod_problema_pj = 1
(6, 5, 2, TRUE, TRUE, 6, 5.00, 3, 3.00, 1, NULL,
 'PJ sem contrato formal. Horas extras são incertas.', 3),

-- User 7, Emp 1 (PJ) => cod_problema_pj = 3
(7, 1, 5, TRUE, FALSE, 9, 8.00, 5, 4.00, 3, NULL,
 'Não pagaram 13° e férias, mesmo exigindo dedicação exclusiva.', 2),

-- User 8, Emp 4 (PJ) => cod_problema_pj = 4
(8, 4, 6, FALSE, NULL, NULL, NULL, NULL, NULL, 4, NULL,
 'PJ total, sem qualquer suporte trabalhista.', 3),

-- User 9, Emp 5 (PJ) => cod_problema_pj = 2
(9, 5, 3, TRUE, TRUE, 18, 15.00, 9, 7.00, 2, NULL,
 'Exigem registro de ponto e metas típicas de CLT.', 3),

-- User 10, Emp 1 (PJ) => cod_problema_pj = 1
(10, 1, 2, FALSE, NULL, NULL, NULL, NULL, NULL, 1, NULL,
 'Contrato PJ com muitas irregularidades. Salário baixo.', 3),

-- User 11, Emp 2 (PJ) => cod_problema_pj = 4
(11, 2, 2, TRUE, TRUE, 10, 7.00, 4, 4.00, 4, NULL,
 'Falta de garantias mínimas para pausa ou licenças.', 3),

-- User 12, Emp 4 (PJ) => cod_problema_pj = 2
(12, 4, 3, TRUE, FALSE, 7, 5.00, 3, 3.00, 2, NULL,
 'Vínculo disfarçado; tratam como funcionário CLT.', 2),

-- User 13, Emp 2 (PJ) => cod_problema_pj = 3
(13, 2, 1, FALSE, NULL, NULL, NULL, NULL, NULL, 3, NULL,
 'PJ sem férias ou 13°, trabalho integral.', 3),

-- User 14, Emp 1 (PJ) => cod_problema_pj = 1
(14, 1, 6, TRUE, TRUE, 8, 8.00, 4, 4.00, 1, NULL,
 'Disseram ser PJ flexível, mas contrato é confuso.', 3),

-- User 15, Emp 5 (PJ) => cod_problema_pj = 4
(15, 5, 5, FALSE, NULL, NULL, NULL, NULL, NULL, 4, NULL,
 'Regras trabalhistas inexistentes, tudo por conta própria.', 3);

------------------------------------------------------------------------------
-- 8. INSERÇÃO DE BENEFÍCIOS PARA AS AVALIAÇÕES (2 BENEFÍCIOS POR AVALIAÇÃO = 60 INSERÇÕES)
------------------------------------------------------------------------------

-- Precisamos saber quantas avaliações foram criadas acima (30).
-- Cada avaliação recebeu um ID sequencial (1 a 30). Adicionaremos 2 benefícios para cada.

INSERT INTO tbl_avaliacao_beneficio (id_avaliacao, id_beneficio)
VALUES
-- Para cada id_avaliacao de 1..30, associamos 2 benefícios
(1, 1),
(1, 2),
(2, 2),
(2, 3),
(3, 1),
(3, 3),
(4, 2),
(4, 3),
(5, 1),
(5, 3),
(6, 3),
(6, 2),
(7, 1),
(7, 2),
(8, 3),
(8, 1),
(9, 1),
(9, 3),
(10, 2),
(10, 3),
(11, 1),
(11, 2),
(12, 2),
(12, 3),
(13, 1),
(13, 2),
(14, 2),
(14, 3),
(15, 1),
(15, 3),
(16, 3),
(16, 1),
(17, 1),
(17, 2),
(18, 2),
(18, 3),
(19, 1),
(19, 3),
(20, 1),
(20, 2),
(21, 2),
(21, 3),
(22, 1),
(22, 3),
(23, 1),
(23, 2),
(24, 2),
(24, 3),
(25, 1),
(25, 3),
(26, 1),
(26, 2),
(27, 2),
(27, 3),
(28, 1),
(28, 3),
(29, 2),
(29, 3),
(30, 1),
(30, 2);
    """
VIEWS = """CREATE TABLE mv_usuario_empresa_atual AS
SELECT u.id_usuario,
       u.nm_usuario,
       e.id_empresa,
       e.nm_fantasia_empresa,
       v.dt_inicio_vinculo,
       v.salario_vinculo
FROM tbl_vinculo_usuario_empresa v
JOIN tbl_usuario u ON v.id_usuario = u.id_usuario
JOIN tbl_empresa e ON v.id_empresa = e.id_empresa
WHERE v.emprego_atual = TRUE;

CREATE TABLE mv_empresa_resumo_avaliacao AS  
SELECT a.id_avaliacao,  
       a.id_empresa,  
       u.id_usuario,  
       u.nm_usuario usuario,  
       e.nm_fantasia_empresa,  
       a.depoimento_geral,  
       a.nota_geral,  
       a.dt_avaliacao  
FROM tbl_avaliacao a  
JOIN tbl_empresa e ON a.id_empresa = e.id_empresa  
JOIN tbl_usuario u on u.id_usuario = a.id_usuario;

CREATE OR REPLACE VIEW vw_beneficios_mais_oferecidos_boas_notas AS
SELECT
b.ds_beneficio,
COUNT(*) AS qtd
FROM tbl_avaliacao a
JOIN tbl_avaliacao_beneficio ab ON a.id_avaliacao = ab.id_avaliacao
JOIN tbl_beneficio b ON ab.id_beneficio = b.id_beneficio
WHERE a.nota_geral >= 4
GROUP BY b.ds_beneficio
ORDER BY qtd DESC;

CREATE VIEW vw_media_salario_cargo_senioridade AS
SELECT 
    ce.ds_cargo_especialidade AS cargo,
    s.ds_senioridade          AS senioridade,
    ROUND(AVG(vue.salario_vinculo)::numeric, 2) AS media_salarial
FROM tbl_vinculo_usuario_empresa vue
JOIN tbl_cargo_especialidade ce ON vue.id_cargo_especialidade = ce.id_cargo_especialidade
JOIN tbl_senioridade s         ON vue.id_senioridade = s.id_senioridade
GROUP BY ce.ds_cargo_especialidade, s.ds_senioridade;

CREATE VIEW vw_problemas_pj AS
SELECT
    CASE a.cod_problema_pj
      WHEN 1 THEN 'Irregularidades contratuais'
      WHEN 2 THEN 'CLT mascarado de PJ'
      WHEN 3 THEN 'Não pagamento de férias ou 13°'
      WHEN 4 THEN 'Falta de garantia de direitos trabalhistas'
      ELSE 'Nenhum/Indefinido'
    END AS problema_pj,
    COUNT(*) AS qtd
FROM tbl_avaliacao a
WHERE a.cod_problema_pj IS NOT NULL
GROUP BY a.cod_problema_pj
ORDER BY qtd DESC;

CREATE VIEW vw_distribuicao_clt_pj AS
SELECT 
    e.nm_fantasia_empresa AS empresa,
    SUM(CASE WHEN vue.cod_regime_contratacao = 1 THEN 1 ELSE 0 END) AS total_clt,
    SUM(CASE WHEN vue.cod_regime_contratacao = 2 THEN 1 ELSE 0 END) AS total_pj
FROM tbl_vinculo_usuario_empresa vue
JOIN tbl_empresa e ON vue.id_empresa = e.id_empresa
GROUP BY e.nm_fantasia_empresa
ORDER BY empresa;

CREATE VIEW vw_tempo_medio_promocao_senioridade AS
SELECT
    s.ds_senioridade AS senioridade,
    ROUND(AVG(a.tempo_primeira_promocao)::numeric, 2) AS media_meses_promocao
FROM tbl_avaliacao a
JOIN tbl_usuario u ON a.id_usuario = u.id_usuario
JOIN tbl_vinculo_usuario_empresa vue 
  ON a.id_usuario = vue.id_usuario
 AND a.id_empresa = vue.id_empresa
JOIN tbl_senioridade s ON vue.id_senioridade = s.id_senioridade
WHERE a.tempo_primeira_promocao IS NOT NULL
GROUP BY s.ds_senioridade
ORDER BY media_meses_promocao;

 CREATE VIEW vw_empresas_clt_mascarado_pj AS
SELECT 
    e.nm_fantasia_empresa AS empresa,
    COUNT(*) AS qtd_clt_mascarado
FROM tbl_avaliacao a
JOIN tbl_empresa e ON a.id_empresa = e.id_empresa
WHERE a.cod_problema_pj = 2
GROUP BY e.nm_fantasia_empresa
ORDER BY qtd_clt_mascarado DESC;

CREATE VIEW vw_empresas_satisfacao_alta AS
SELECT 
    e.nm_fantasia_empresa AS empresa,
    ROUND(AVG(a.nota_geral)::numeric, 2) AS media_nota
FROM tbl_avaliacao a
JOIN tbl_empresa e ON a.id_empresa = e.id_empresa
GROUP BY e.nm_fantasia_empresa
HAVING AVG(a.nota_geral) > 4
ORDER BY media_nota DESC;

CREATE VIEW vw_tempo_primeiro_aumento_dev AS
SELECT
    s.ds_senioridade AS senioridade,
    ROUND(AVG(a.tempo_primeiro_aumento)::numeric, 2) AS media_meses_aumento
FROM tbl_avaliacao a
JOIN tbl_vinculo_usuario_empresa vue 
  ON a.id_usuario = vue.id_usuario 
 AND a.id_empresa = vue.id_empresa
JOIN tbl_cargo_especialidade ce ON vue.id_cargo_especialidade = ce.id_cargo_especialidade
JOIN tbl_senioridade s         ON vue.id_senioridade = s.id_senioridade
WHERE ce.ds_cargo_especialidade ILIKE '%desenvolvedor%'
  AND s.ds_senioridade IN ('Júnior', 'Pleno', 'Sênior')
  AND a.tempo_primeiro_aumento IS NOT NULL
GROUP BY s.ds_senioridade
ORDER BY s.ds_senioridade;

CREATE VIEW vw_empresas_maior_retencao AS
SELECT 
    e.nm_fantasia_empresa AS empresa,
    COUNT(*) AS total_emprego_atual
FROM tbl_vinculo_usuario_empresa vue
JOIN tbl_empresa e ON vue.id_empresa = e.id_empresa
WHERE vue.emprego_atual = TRUE
GROUP BY e.nm_fantasia_empresa
ORDER BY total_emprego_atual DESC;

DROP VIEW IF EXISTS vw_percentual_hora_extra_remunerada;

CREATE VIEW vw_percentual_hora_extra_remunerada AS
WITH base AS (
  SELECT
    (SELECT COUNT(*) FROM tbl_empresa) AS total_emp,

    (SELECT COUNT(DISTINCT a.id_empresa)
       FROM tbl_avaliacao a
      WHERE a.faz_hora_extra = TRUE
        AND a.hora_extra_remunerada = TRUE
    ) AS total_rem,

    (SELECT COUNT(DISTINCT a.id_empresa)
       FROM tbl_avaliacao a
      WHERE a.faz_hora_extra = TRUE
        AND a.hora_extra_remunerada = FALSE
    ) AS total_nao_rem
)
SELECT 'Remunerado' AS categoria,
       CASE WHEN total_emp = 0 THEN 0
            ELSE ROUND((total_rem * 100.0 / total_emp), 2)
       END AS percentual
FROM base

UNION ALL

SELECT 'Não Remunerado' AS categoria,
       CASE WHEN total_emp = 0 THEN 0
            ELSE ROUND((total_nao_rem * 100.0 / total_emp), 2)
       END AS percentual
FROM base;
"""
def query_to_dict(conn, sql):
    query_result = conn.query(sql)
    query_result_data = query_result.fetchall()
    final_result = []
    cols = [col[0] for col in query_result.description]
    for i in query_result_data:
        final_result.append(dict(zip(cols, i)))

    return final_result


def create ():
    db_file = 'meu_banco.duckdb'

    if os.path.exists(db_file):
            os.remove(db_file)

    conn = duckdb.connect(db_file)

    print(f"Conectado ao arquivo DuckDB: {db_file}")

    conn.execute(DDL)
    print("DDL executado com sucesso!")

    conn.execute(POPULATE)
    print("Banco de dados populado")
    conn.execute(VIEWS)
    print("VIEWS criadas")



    query_ex = r"""
    select * 
    from vw_media_salario_cargo_senioridade
            """
    result = conn.query(query_ex).execute().fetchall()
    print(result)
    plt.barh([x[0] + ' '+x[1] for x in result],[x[2] for x in result])
    plt.show()

    
    query_ex = r"""
    select * 
    from vw_beneficios_mais_oferecidos_boas_notas
            """
    result = conn.query(query_ex).execute().fetchall()
    print(result)
    plt.bar([x[0]for x in result],[x[1] for x in result])
    plt.show()


    query_ex = r"""
    select * 
    from vw_problemas_pj
            """
    result = conn.query(query_ex).execute().fetchall()
    print(result)
    plt.barh([x[0]for x in result],[x[1] for x in result])
    plt.show()

    tables = conn.execute("SHOW TABLES").fetchall()
    print("Tabelas criadas:")
    for t in tables:
        print(" -", t[0])
    conn.close()



if __name__ == "__main__":
    create()

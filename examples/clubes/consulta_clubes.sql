select distinct substr(cnpj,1,8) as cnpj_matriz
from cnpj_dados_cadastrais_pj c
left outer join tab_cnae cnae on c.cnae_fiscal = cnae.cod_cnae
left outer join tab_natureza_juridica nj on c.codigo_natureza_juridica = nj.cod_subclass_natureza_juridica

-- Deve ser estabelecimento matriz
where c.identificador_matriz_filial = '1'
-- CNAE principal ou secundária = 'clubes sociais, esportivos ou similares'
and (c.cnae_fiscal = '9312300' or c.cnpj in (select cnpj from cnpj_dados_cnae_secundario_pj where cnae_secundario = '9312300'))
-- CNAE principal deve estar neste conjunto
and c.cnae_fiscal in ('7490105','7721700','8591100','9311500','9312300','9319101','9319199','9329899','9412099','9499500')
-- Exclui natureza jurídica de administração pública e algumas outras
and c.codigo_natureza_juridica not like '1%'
and c.codigo_natureza_juridica not in ('2011','2143','3085','3115','3131','3220','3247','3271')
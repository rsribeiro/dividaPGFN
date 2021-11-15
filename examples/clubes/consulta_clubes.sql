select
	distinct emp.cnpj_basico
	--emp.cnpj_basico, est.cnpj, emp.razao_social, est.nome_fantasia, emp.natureza_juridica, est.cnae_fiscal, est.cnae_fiscal_secundaria
from empresas emp
left outer join estabelecimento est on emp.cnpj_basico = est.cnpj_basico
--left outer join natureza_juridica nj on emp.natureza_juridica = nj.codigo
--left outer join cnae cnae on est.cnae_fiscal = cnae.codigo

-- Deve ser estabelecimento matriz
where est.matriz_filial = '1'
-- CNAE principal ou secundária = 'clubes sociais, esportivos ou similares'
and (est.cnae_fiscal = '9312300' or est.cnae_fiscal_secundaria like '%9312300%')
-- CNAE principal deve estar neste conjunto
and est.cnae_fiscal in ('7490105','7721700','8591100','9311500','9312300','9319101','9319199','9329899','9412099','9499500')
-- Exclui natureza jurídica de administração pública e algumas outras
and emp.natureza_juridica not like '1%'
and emp.natureza_juridica not in ('2011','2143','3085','3115','3131','3220','3247','3271')
--limit 10
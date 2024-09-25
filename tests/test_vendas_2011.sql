 /* Teste de vendas brutas no ano de 2011 - Auditado por Carlos 
 Total = $12.646.112,16 */

select count(*)
from {{ ref('vendas_2011') }}
where vendas_brutas <> 12646112.1607


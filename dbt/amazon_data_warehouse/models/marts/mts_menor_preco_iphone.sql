with source as (
    select pais, sum(num_avaliacoes_produto) as avaliacoes
    from i{{ref("int_produtos")}}
    group by pais;
)

select *
from source
package com.ric.dividapgfn.linha;

import java.math.BigDecimal;

public record LinhaFGTS(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
		String ufUnidadeResponsavel, String unidadeResponsavel, String entidadeResponsavel, String unidadeInscricao,
		String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao, String receitaPrincipal,
		String dataInscricao, String indicadorAjuizado, BigDecimal valor) {
}

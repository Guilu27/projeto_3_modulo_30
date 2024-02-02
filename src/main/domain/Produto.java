/**
 * 
 */
package main.domain;

import java.math.BigDecimal;

import main.anotacao.ColunaTabela;
import main.anotacao.Tabela;
import main.anotacao.TipoChave;
import main.dao.Persistente;

/**
 * @author rodrigo.pires
 *
 */
@Tabela("TB_PRODUTO")
public class Produto implements Persistente {
	
	@ColunaTabela(dbName = "id", setJavaName = "setId")
	private Long id;

	@TipoChave("getCodigo")
	@ColunaTabela(dbName = "codigo", setJavaName = "setCodigo")
	private String codigo;
	
	@ColunaTabela(dbName = "nome", setJavaName = "setNome")
	private String nome;
	
	@ColunaTabela(dbName = "descricao", setJavaName = "setDescricao")
	private String descricao;
	
	@ColunaTabela(dbName = "valor", setJavaName = "setValor")
	private BigDecimal valor;

	@ColunaTabela(dbName = "peso_kg", setJavaName = "setPesoKg")
	private BigDecimal pesoKg;

	private Estoque estoque;

	public String getCodigo() {
		return codigo;
	}

	public void setCodigo(String codigo) {
		this.codigo = codigo;
	}

	public String getNome() {
		return nome;
	}

	public void setNome(String nome) {
		this.nome = nome;
	}

	public String getDescricao() {
		return descricao;
	}

	public void setDescricao(String descricao) {
		this.descricao = descricao;
	}

	public BigDecimal getValor() {
		return valor;
	}

	public void setValor(BigDecimal valor) {
		this.valor = valor;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public BigDecimal getPesoKg() {
		return pesoKg;
	}

	public void setPesoKg(BigDecimal pesoKg) {
		this.pesoKg = pesoKg;
	}

	public Estoque getEstoque() {
		return estoque;
	}

	public void setEstoque(Estoque estoque) {
		this.estoque = estoque;
	}
}

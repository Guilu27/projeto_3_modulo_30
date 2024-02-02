/**
 * 
 */
package main.dao;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import main.anotacao.ColunaTabela;
import main.dao.factory.EstoqueFactory;
import main.dao.factory.ProdutoFactory;
import main.dao.factory.ProdutoQuantidadeFactory;
import main.dao.factory.VendaFactory;
import main.dao.generic.GenericDAO;
import main.domain.Estoque;
import main.domain.Produto;
import main.domain.ProdutoQuantidade;
import main.domain.Venda;
import main.exceptions.*;

/**
 * @author rodrigo.pires
 *
 */
public class ProdutoDAO extends GenericDAO<Produto, String> implements IProdutoDAO {
	
	public ProdutoDAO() {
		super();
	}

	@Override
	public Class<Produto> getTipoClasse() {
		return Produto.class;
	}

	@Override
	public void atualiarDados(Produto entity, Produto entityCadastrado) {
		entityCadastrado.setCodigo(entity.getCodigo());
		entityCadastrado.setDescricao(entity.getDescricao());
		entityCadastrado.setNome(entity.getNome());
		entityCadastrado.setValor(entity.getValor());
		entityCadastrado.setPesoKg(entity.getPesoKg());
	}

	@Override
	protected String getQueryInsercao() {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO TB_PRODUTO ");
		sb.append("(ID, CODIGO, NOME, DESCRICAO, VALOR, PESO_KG)");
		sb.append("VALUES (nextval('sq_produto'),?,?,?,?,?)");
		return sb.toString();
	}

	@Override
	protected void setParametrosQueryInsercao(PreparedStatement stmInsert, Produto entity) throws SQLException {
		stmInsert.setString(1, entity.getCodigo());
		stmInsert.setString(2, entity.getNome());
		stmInsert.setString(3, entity.getDescricao());
		stmInsert.setBigDecimal(4, entity.getValor());
		stmInsert.setBigDecimal(5,entity.getPesoKg());
	}

	@Override
	protected String getQueryExclusao() {
		return "DELETE FROM TB_PRODUTO WHERE CODIGO = ?";
	}

	@Override
	protected void setParametrosQueryExclusao(PreparedStatement stmExclusao, String valor) throws SQLException {
		stmExclusao.setString(1, valor);
	}

	@Override
	protected String getQueryAtualizacao() {
		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE TB_PRODUTO ");
		sb.append("SET CODIGO = ?, ");
		sb.append("NOME = ?, ");
		sb.append("DESCRICAO = ?, ");
		sb.append("VALOR = ?, ");
		sb.append("PESO_KG = ? ");
		sb.append(" WHERE CODIGO = ?");
		return sb.toString();
	}

	@Override
	protected void setParametrosQueryAtualizacao(PreparedStatement stmUpdate, Produto entity) throws SQLException {
		stmUpdate.setString(1, entity.getCodigo());
		stmUpdate.setString(2, entity.getNome());
		stmUpdate.setString(3, entity.getDescricao());
		stmUpdate.setBigDecimal(4, entity.getValor());
		stmUpdate.setBigDecimal(5,entity.getPesoKg());
		stmUpdate.setString(6, entity.getCodigo());
	}

	@Override
	protected void setParametrosQuerySelect(PreparedStatement stmExclusao, String valor) throws SQLException {
		stmExclusao.setString(1, valor);
	}

	@Override
	public Boolean cadastrar(Produto entity) throws TipoChaveNaoEncontradaException, DAOException {
		Connection connection = null;
		PreparedStatement stm = null;
		try {
			connection = getConnection();
			stm = connection.prepareStatement(getQueryInsercao(), Statement.RETURN_GENERATED_KEYS);
			setParametrosQueryInsercao(stm, entity);
			int rowsAffected = stm.executeUpdate();

			if(rowsAffected > 0) {
				try (ResultSet rs = stm.getGeneratedKeys()){
					if (rs.next()) {
						entity.setId(rs.getLong(1));
					}
				}
				stm = connection.prepareStatement(getQueryInsercaoEstoque());
				setParametrosQueryInsercaoEstoque(stm, entity);
				rowsAffected = stm.executeUpdate();

				return true;
			}

		} catch (SQLException e) {
			throw new DAOException("ERRO CADASTRANDO OBJETO ", e);
		} finally {
			closeConnection(connection, stm, null);
		}
		return false;
}

	private void setParametrosQueryInsercaoEstoque(PreparedStatement stm, Produto entity) throws SQLException {
		stm.setLong(1,entity.getId());
		stm.setInt(2, entity.getEstoque().getQuantidadeEstoque());
	}

	private String getQueryInsercaoEstoque() {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO TB_ESTOQUE ");
		sb.append("(ID, ID_PRODUTO_FK, QUANTIDADE_PRODUTO) ");
		sb.append("VALUES (nextval('sq_estoque'),?,?)");
		return sb.toString();
	}

	@Override
	public void excluir(String valor) throws DAOException {
		Connection connection = getConnection();
		PreparedStatement stm = null;
		try {
			stm = connection.prepareStatement(getQueryExclusaoEstoque());
			setParametrosQueryExclusaoEstoque(stm, valor);
			int rowsAffected = stm.executeUpdate();
			
			stm = connection.prepareStatement(getQueryExclusao());
			setParametrosQueryExclusao(stm, valor);
			rowsAffected = stm.executeUpdate();

		} catch (SQLException e) {
			throw new DAOException("ERRO EXCLUINDO OBJETO ", e);
		} finally {
			closeConnection(connection, stm, null);
		}
	}

	private String getQueryExclusaoEstoque() {
		return "DELETE FROM TB_ESTOQUE WHERE ID_PRODUTO_FK IN (SELECT ID FROM TB_PRODUTO WHERE CODIGO = ?);";
	}

	private void setParametrosQueryExclusaoEstoque(PreparedStatement stm, String valor) throws SQLException {
		stm.setString(1, valor);
	}

	@Override
	public Produto consultar(String valor) throws MaisDeUmRegistroException, TableException, DAOException {
		StringBuilder sb = sqlSelect();
		sb.append("WHERE CODIGO = ? ");

		Connection connection = null;
		PreparedStatement stm = null;
		ResultSet rs = null;

		try {
			connection = getConnection();
			stm = connection.prepareStatement(sb.toString());
			setParametrosQuerySelect(stm, valor);
			rs = stm.executeQuery();
			if (rs.next()) {
				Produto produto = ProdutoFactory.convert(rs);
				buscarAssociacaoProdutoEstoque(connection, produto);
				return produto;
			}

		} catch (SQLException e) {
			throw new DAOException("ERRO CONSULTANDO OBJETO ", e);
		} finally {
			closeConnection(connection, stm, rs);
		}
		return null;
	}

	private void buscarAssociacaoProdutoEstoque(Connection connection, Produto produto) throws DAOException {
		PreparedStatement stm = null;
		ResultSet rs = null;
		try {
			stm = connection.prepareStatement(getQueryConsultaEstoque());
			stm.setString(1, produto.getCodigo());
			rs = stm.executeQuery();

			if (rs.next()) {
				Estoque estoque = EstoqueFactory.convert(rs);

				produto.setEstoque(estoque);
			}

		} catch (SQLException e) {
			throw new DAOException("ERRO CONSULTANDO OBJETO ", e);
		} finally {
			closeConnection(connection, stm, rs);
		}
	}

	private StringBuilder sqlSelect() {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT *, ID AS ID_PRODUTO ");
		sb.append("FROM TB_PRODUTO ");
		return sb;
	}

	private String getQueryConsultaEstoque (){
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT P.ID AS ID_PRODUTO, P.CODIGO, P.NOME, P.DESCRICAO, P.VALOR, P.PESO_KG, ");
		sb.append("E.ID AS ID_ESTOQUE, E.QUANTIDADE_PRODUTO ");
		sb.append("FROM TB_PRODUTO P INNER JOIN TB_ESTOQUE E ON P.ID = E.ID_PRODUTO_FK ");
		sb.append("WHERE P.CODIGO = ?");
		return sb.toString();
	}

	@Override
	public void alterar(Produto entity) throws TipoChaveNaoEncontradaException, DAOException {

		Connection connection = getConnection();
		PreparedStatement stm = null;
		try {
			stm = connection.prepareStatement(getQueryAtualizacao());
			setParametrosQueryAtualizacao(stm, entity);
			int rowsAffected = stm.executeUpdate();

			stm = connection.prepareStatement(getQueryAtualizacaoEstoque());
			setParametrosQueryAtualizacaoEstoque(stm, entity);
			rowsAffected = stm.executeUpdate();
		} catch (SQLException e) {
			throw new DAOException("ERRO ALTERANDO OBJETO ", e);
		} finally {
			closeConnection(connection, stm, null);
		}
	}

	private void setParametrosQueryAtualizacaoEstoque(PreparedStatement stm, Produto entity) throws SQLException {
		stm.setInt(1, entity.getEstoque().getQuantidadeEstoque());
		stm.setLong(2,entity.getId());
	}

	private String getQueryAtualizacaoEstoque() {
		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE TB_ESTOQUE ");
		sb.append("SET QUANTIDADE_PRODUTO = ? ");
		sb.append("WHERE ID_PRODUTO_FK = ?");
		return sb.toString();
	}
}

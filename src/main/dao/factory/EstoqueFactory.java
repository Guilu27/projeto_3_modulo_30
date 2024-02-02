package main.dao.factory;

import main.domain.Estoque;
import main.domain.Produto;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EstoqueFactory {

    public static Estoque convert(ResultSet rs) throws SQLException {
        Produto produto = ProdutoFactory.convert(rs);
        Estoque estoque = new Estoque();
        estoque.setId(rs.getLong("ID_ESTOQUE"));
        estoque.setProduto(produto);
        estoque.setQuantidadeEstoque(rs.getInt("QUANTIDADE_PRODUTO"));
        return estoque;
    }
}

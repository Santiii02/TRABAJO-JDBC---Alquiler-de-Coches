package lsi.ubu.servicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;

/**
 * @author <a> Alberto Lanchares Diez</a>
 * @author <a> Andres Puentes Gonzalez</a>
 * @author <a> Santiago Infante Ramos</a>
 */

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4;

	@SuppressWarnings("unused")
	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		long diasDiff = DIAS_DE_ALQUILER; 
		if (fechaFin != null) {
		    diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());
		    
		    if (diasDiff < 1) {
		        throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
		    }
		} else {
		    // Validación adicional requerida por el test
		    if (DIAS_DE_ALQUILER < 1) {
		        throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
		    }
		}

		// Cálculo de fechaFinal (independientemente de fechaFin)
		Date fechaFinal = fechaFin != null ? fechaFin : 
		    new Date(fechaIni.getTime() + TimeUnit.DAYS.toMillis(DIAS_DE_ALQUILER));

		try {
			con = pool.getConnection();

			/* A completar por el alumnado... */

			con.setAutoCommit(false); // Iniciamos transacción

			// Verificar existencia del vehículo
			pstmt = con.prepareStatement("SELECT 1 FROM Vehiculos WHERE matricula = ?");
			pstmt.setString(1, matricula);
			rs = pstmt.executeQuery();
			if (!rs.next()) {
			    throw new AlquilerCochesException(2);
			}

			// Verificar existencia del cliente
			pstmt = con.prepareStatement("SELECT 1 FROM Clientes WHERE NIF = ?");
			pstmt.setString(1, nifCliente);
			rs = pstmt.executeQuery();
			if (!rs.next()) {
			    throw new AlquilerCochesException(1);
			}

	        // Verificar disponibilidad del vehículo
	        pstmt = con.prepareStatement(" SELECT 1 FROM Reservas WHERE matricula = ? AND ((fecha_ini BETWEEN ? AND ?) OR (fecha_fin BETWEEN ? AND ?) OR (? BETWEEN fecha_ini AND fecha_fin))");
	        
	        pstmt.setString(1, matricula);
	        pstmt.setDate(2, new java.sql.Date(fechaIni.getTime()));
	        pstmt.setDate(3, new java.sql.Date(fechaFinal.getTime()));
	        pstmt.setDate(4, new java.sql.Date(fechaIni.getTime()));
	        pstmt.setDate(5, new java.sql.Date(fechaFinal.getTime()));
	        pstmt.setDate(6, new java.sql.Date(fechaIni.getTime()));
	        
	        rs = pstmt.executeQuery();
	        if (rs.next()) {
	            throw new AlquilerCochesException(4);
	        }

	        // Insertar reserva
	        pstmt = con.prepareStatement("INSERT INTO Reservas(idReserva, cliente, matricula, fecha_ini, fecha_fin) VALUES (seq_reservas.NEXTVAL, ?, ?, ?, ?)");
	        
	        pstmt.setString(1, nifCliente);
	        pstmt.setString(2, matricula);
	        pstmt.setDate(3, new java.sql.Date(fechaIni.getTime()));
	        if (fechaFin != null) {
	            pstmt.setDate(4, new java.sql.Date(fechaFinal.getTime()));
	        } else {
	            pstmt.setNull(4, java.sql.Types.DATE); // Insertar null en fecha_fin
	        }	        pstmt.executeUpdate();

	        // Generar factura (requerido para pasar los tests)
	        pstmt = con.prepareStatement(
	        	    "SELECT m.id_modelo, m.precio_cada_dia, m.capacidad_deposito, pc.precio_por_litro, pc.tipo_combustible " +
	        	    "FROM Modelos m " +
	        	    "JOIN Precio_Combustible pc ON m.tipo_combustible = pc.tipo_combustible " +
	        	    "JOIN Vehiculos v ON m.id_modelo = v.id_modelo " +
	        	    "WHERE v.matricula = ?"
	        );


	        pstmt.setString(1, matricula);
	        rs = pstmt.executeQuery();
	        
	        if (rs.next()) {
	            int idModelo = rs.getInt(1); // id_modelo
	            BigDecimal precioDia = rs.getBigDecimal(2); // precio_cada_dia
	            int capacidad = rs.getInt(3); // capacidad_deposito
	            BigDecimal precioCombustible = rs.getBigDecimal(4); // precio_por_litro
	            String tipoCombustible = rs.getString(5); // tipo_combustible
	            
	            BigDecimal importeAlquiler = precioDia.multiply(BigDecimal.valueOf(diasDiff));
	            BigDecimal importeCombustible = new BigDecimal(capacidad).multiply(precioCombustible);
	            
	            // Insertar factura
	            pstmt = con.prepareStatement(
	                "INSERT INTO Facturas VALUES (seq_num_fact.NEXTVAL, ?, ?)",
	                new String[]{"nroFactura"});
	            
	            pstmt.setBigDecimal(1, importeAlquiler.add(importeCombustible));
	            pstmt.setString(2, nifCliente);
	            pstmt.executeUpdate();
	            
	            // Insertar líneas de factura
	            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
	                if (generatedKeys.next()) {
	                    int nroFactura = generatedKeys.getInt(1);
	                    
	                    pstmt = con.prepareStatement(
	                        "INSERT INTO Lineas_Factura VALUES (?, ?, ?)");
	                    pstmt.setInt(1, nroFactura);
	                    pstmt.setString(2, diasDiff + " dias de alquiler, vehiculo modelo " + idModelo);
	                    pstmt.setBigDecimal(3, importeAlquiler);
	                    pstmt.executeUpdate();
	                    
	                    // Línea 2: Depósito
	                    pstmt.setString(2, "Deposito lleno de " + capacidad + " litros de " + tipoCombustible);
	                    pstmt.setBigDecimal(3, importeCombustible);
	                    pstmt.executeUpdate();
	                }
	            }
	        }

	        con.commit();

	    } catch (AlquilerCochesException e) {
	        if (con != null) con.rollback();
	        throw e;
	    } catch (SQLException e) {
	        LOGGER.error("Error SQL: " + e.getMessage());
	        if (con != null) con.rollback();
	        throw e;
	    } finally {
	        try { if (rs != null) rs.close(); } catch (SQLException e) { LOGGER.error(e.getMessage()); }
	        try { if (pstmt != null) pstmt.close(); } catch (SQLException e) { LOGGER.error(e.getMessage()); }
	        try { if (con != null) con.close(); } catch (SQLException e) { LOGGER.error(e.getMessage()); }
	    }
	}
}

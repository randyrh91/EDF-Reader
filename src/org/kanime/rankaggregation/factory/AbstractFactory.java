package org.kanime.rankaggregation.factory;

import java.net.URL;

import org.knime.core.node.ExecutionContext;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 */

public abstract class AbstractFactory {

	public abstract EDFDataTable createDataTable(URL url, String prefix, ExecutionContext exec) throws Exception;
}

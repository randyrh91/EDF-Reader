package org.knime.rankaggregation.factory;

import org.knime.rankaggregation.tables.AbstractEDFFile;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 */

public abstract class AbstractEDFFactory {

	public abstract AbstractEDFFile readData(String extension) throws Exception;
}

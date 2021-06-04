package org.kanime.rankaggregation.factory;

import java.net.URL;
import org.apache.commons.io.FilenameUtils;
import org.knime.core.node.ExecutionContext;
import org.knime.rankaggregation.tables.DataTableMjg;
import org.knime.rankaggregation.tables.DataTablePwg;
import org.knime.rankaggregation.tables.DataTableSoc;
import org.knime.rankaggregation.tables.DataTableSoi;
import org.knime.rankaggregation.tables.DataTableToc;
import org.knime.rankaggregation.tables.DataTableTog;
import org.knime.rankaggregation.tables.DataTableToi;
import org.knime.rankaggregation.tables.DataTableWmg;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 */

public final class EDFFactory extends AbstractFactory {

	EDFDataTable EDFtable;
	public EDFFactory() {
	}

	@Override
	public EDFDataTable createDataTable(URL url, String prefix, ExecutionContext exec) throws Exception {
		String extension = getFileExtension(url);
		switch (extension) {
		case "mjg": {
			return new DataTableMjg(url, prefix, exec);
		}
		case "pwg": {
			return new DataTablePwg(url, prefix, exec);
		}
		case "soc": {
			return new DataTableSoc(url, prefix, exec);
		}
		case "soi": {
			return new DataTableSoi(url, prefix, exec);
		}
		case "toc": {
			return new DataTableToc(url, prefix, exec);
		}
		case "tog": {
			return new DataTableTog(url, prefix, exec);
		}
		case "toi": {
			return new DataTableToi(url, prefix, exec);
		}
		case "wmg": {
			return new DataTableWmg(url, prefix, exec);
		}
		default:
			return new DataTableToi(url, prefix, exec);
		}
	}

	private String getFileExtension(URL url) {
		String extension = FilenameUtils.getExtension(url.getPath());
		return extension;
	}

}

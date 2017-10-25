package com.impetus.eth.jdbc;

import java.util.ArrayList;
import java.util.List;

public interface DataHandler {
	
	public ArrayList<Object[]> convertToObjArray(List<?> rows);

}

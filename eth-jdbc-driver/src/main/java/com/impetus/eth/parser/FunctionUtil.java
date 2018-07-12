/******************************************************************************* 
 * * Copyright 2018 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.eth.parser;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.function.Parameters;
import com.impetus.blkch.sql.query.BytesArgs;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.ListAgrs;
import com.impetus.blkch.sql.smartcontract.SmartCnrtListType;
import com.impetus.blkch.util.Utilities;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionUtil {

	public static void handleIdent(IdentifierNode ident, List<Object> args, List<Class> argsType) {
		switch (ident.getType()) {
		case STRING:
			argsType.add(String.class);
			args.add(Utilities.unquote(ident.getValue()));
			break;
		case BOOLEAN:
			argsType.add(Boolean.class);
			Boolean boolVal = ident.getValue().equalsIgnoreCase("true");
			args.add(boolVal);
			break;
		case NUMBER:
			argsType.add(BigInteger.class);
			args.add(new BigInteger(ident.getValue()));
			break;
		default:
			throw new BlkchnException("Can not handle unknown Data type");
		}
	}

	public static void handleList(ListAgrs lst, List<Object> args, List<Class> argsType) {
		if (lst.hasChildType(SmartCnrtListType.class) && lst.hasChildType(Parameters.class)) {
			String lsTyp = Utilities.unquote(lst.getChildType(SmartCnrtListType.class, 0).getListTypeDec());
			if (lsTyp.equalsIgnoreCase("BYTES")) {
				lstRecurBytes(lst.getChildType(BytesArgs.class), args, argsType);
			} else {
				lstRecur(lsTyp, lst.getChildType(Parameters.class, 0).getChildType(IdentifierNode.class), args,
						argsType);
			}
		} else if (!lst.hasChildType(SmartCnrtListType.class) && lst.hasChildType(Parameters.class)) {
			if (lst.getChildType(Parameters.class, 0).hasChildType(BytesArgs.class)) {
				lstRecurBytes(lst.getChildType(Parameters.class, 0).getChildType(BytesArgs.class), args, argsType);
			} else {
				lstRecur(
						lst.getChildType(Parameters.class, 0).getChildType(IdentifierNode.class, 0).getType()
								.toString(),
						lst.getChildType(Parameters.class, 0).getChildType(IdentifierNode.class), args, argsType);
			}
		} else if (lst.hasChildType(SmartCnrtListType.class) && !lst.hasChildType(Parameters.class)) {
			String lsTyp = Utilities.unquote(lst.getChildType(SmartCnrtListType.class, 0).getListTypeDec());
			if (lsTyp.equalsIgnoreCase("BYTES")) {
				lstRecurBytes(new ArrayList<>(), args, argsType);
			} else {
				lstRecur(lsTyp, new ArrayList<>(), args, argsType);
			}
		} else if (!lst.hasChildType(SmartCnrtListType.class) && !lst.hasChildType(Parameters.class)) {
			throw new BlkchnException("Empty list not supported without Data type");
		}
	}

	private static void lstRecur(String lstType, List<IdentifierNode> nodeLst, List<Object> args,
			List<Class> argsType) {
		checkValidList(nodeLst, lstType);
		if (lstType.equalsIgnoreCase("STRING")) {
			List<String> lst = nodeLst.stream().map(e -> Utilities.unquote(e.getValue())).collect(Collectors.toList());
			argsType.add(List.class);
			args.add(lst);
		} else if ((lstType.equalsIgnoreCase("NUMBER") || lstType.equalsIgnoreCase("INTEGER")
				|| lstType.equalsIgnoreCase("BIGINTEGER"))) {
			List<BigInteger> lst = nodeLst.stream().map(e -> new BigInteger(e.getValue())).collect(Collectors.toList());
			argsType.add(List.class);
			args.add(lst);
		} else if (lstType.equalsIgnoreCase("BOOLEAN")) {
			List<Boolean> lst = nodeLst.stream().map(e -> e.getValue().equalsIgnoreCase("true"))
					.collect(Collectors.toList());
			argsType.add(List.class);
			args.add(lst);
		} else {
			throw new BlkchnException("Data Type Not supported for list :: " + lstType
					+ ", only ['BIGINTEGER','INTEGER','BOOLEAN','STRING','BYTES'] allowed");
		}
	}

	private static void lstRecurBytes(List<BytesArgs> nodeLst, List<Object> args, List<Class> argsType) {
		List<byte[]> lst = new ArrayList<>();
		argsType.add(List.class);
		for (BytesArgs byteArg : nodeLst) {
			lst.add(hexStringToByteArray(byteArg.getValue()));
		}
		args.add(lst);
	}

	public static void handleHex(String s, List<Object> args, List<Class> argsType) {
		byte[] data = hexStringToByteArray(s);
		argsType.add(data.getClass());
		args.add(data);
	}

	private static void checkValidList(List<IdentifierNode> nodeLst, String lstType) {
		for (IdentifierNode node : nodeLst) {
			if (!node.getType().toString().equalsIgnoreCase(lstType))
				throw new BlkchnException("List should have same type of data");
		}
	}

	public static byte[] hexStringToByteArray(String s) {
		if (s.startsWith("0x"))
			s = s.replaceAll("0x", "");
		byte[] b = new byte[s.length() / 2];
		for (int i = 0; i < b.length; i++) {
			int index = i * 2;
			int v = Integer.parseInt(s.substring(index, index + 2), 16);
			b[i] = (byte) v;
		}
		return b;
	}
}

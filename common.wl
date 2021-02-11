(* ::Package:: *)

Needs["DatabaseLink`"]


(* ::Subchapter:: *)
(*SQL Table to Dataset*)


(* ::Text:: *)
(*The function sqldataset[sqlConnection, table] creates a Dataset derived from the given table. *)
(*Attributes with a time suffix (such as time_per_element_secs) are renamed without the suffix (e.g. time_per_element) and their components converted to the related Quantity.*)


Module[{mkpattern, patterns}, 
	mkpattern[unit_String, alternatives_List] := <| "unit" -> unit, "pattern" -> StartOfString ~~ (prefix: __) ~~ Verbatim["_"] ~~ RegularExpression["(" <> StringRiffle[alternatives, "|"]  <> ")"] ~~ EndOfString  |>;
	patterns = { 
		mkpattern["Seconds", {"sec", "secs", "seconds"}], 
		mkpattern["Milliseconds", {"millisec", "millisecs", "milliseconds"}], 
		mkpattern["Microseconds", {"usec", "usecs", "microsec", "microsecs", "microseconds"}],
		mkpattern["Nanoseconds", {"nsec", "nsecs", "nanosec", "nanosecs", "nanosecond", "nanoseconds"}]
	};

	sqldataset[conn_SQLConnection, tbl_String] := Module[ 
		{ sqlstmt, resultset, keys, data, transformations, rules},
		sqlstmt = If[ StringMatchQ[tbl,  RegularExpression["^\\w+$"]], "SELECT * FROM " <> tbl, tbl];
		resultset = SQLExecute[conn, sqlstmt, ShowColumnHeadings -> True]; (* the first row are the attribute names of the table *)
		keys = resultset[[1]]; (* list of headers *)
		data = resultset[[2;;]]; (* matrix format *)
		(* the purpose of the following statements is to replace the components whose header is `name_unit' 
		into Quantity[#, "Unit"] with unit in {seconds, milliseconds, microseconds, nanoseconds} and 
		rename the header into `name' *)
		(* first, get a list as { {header1, mapfunction1} , {header2, mapfunction2}, ... } *)
		transformations = Module [{key  = # , scan},
			scan = Scan [ 
			Module[{pattern, unit, repl},
				pattern = #["pattern"];
				unit = #["unit"];
				repl = StringReplace[key, pattern :> prefix , IgnoreCase-> True];
				(*Print["key: ", key, ", repl:", repl, ", unit: ", unit];*)
				If[repl!=  key, 
					Return[ {repl, Quantity[ #, unit] & } ]]
			] &, patterns];
			If[scan =!= Null, scan,
			(* else *) With[{repl = StringReplace[key, StartOfString ~~ (prefix: __) ~~ RegularExpression["_(bool|boolean)$"] :> prefix , IgnoreCase-> True]},
				If[ repl != key, {repl, If[# == 1 || # == "true", True, False] &}, { key, Identity} ]
			]]
		] & /@ keys;
		(* apply the map functions to the columns of the matrix/data *)
		rules = transformations[[All, 2]];
		Do[  
			data[[All, i]] = rules[[i]] /@ data[[All, i]],
			{i, Length[rules]}
		];
		(* rename the headers *)
		keys = transformations[[All, 1]];
		(* finally build the dataset *)
		Dataset[  AssociationThread[ keys, #] & /@ data   ]
	]
]
sqldataset::usage = "Fetch all data from the given table and put into a Dataset.\nUsage: result = sqldataset[conn, tablename]";

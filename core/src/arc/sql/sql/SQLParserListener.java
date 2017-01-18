package com.srotya.sidewinder.core.sql;

// Generated from SQLParser.g4 by ANTLR 4.6


import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SQLParser}.
 */
public interface SQLParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SQLParser#sql}.
	 * @param ctx the parse tree
	 */
	void enterSql(SQLParser.SqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#sql}.
	 * @param ctx the parse tree
	 */
	void exitSql(SQLParser.SqlContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(SQLParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(SQLParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#data_statement}.
	 * @param ctx the parse tree
	 */
	void enterData_statement(SQLParser.Data_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#data_statement}.
	 * @param ctx the parse tree
	 */
	void exitData_statement(SQLParser.Data_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#data_change_statement}.
	 * @param ctx the parse tree
	 */
	void enterData_change_statement(SQLParser.Data_change_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#data_change_statement}.
	 * @param ctx the parse tree
	 */
	void exitData_change_statement(SQLParser.Data_change_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#schema_statement}.
	 * @param ctx the parse tree
	 */
	void enterSchema_statement(SQLParser.Schema_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#schema_statement}.
	 * @param ctx the parse tree
	 */
	void exitSchema_statement(SQLParser.Schema_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#index_statement}.
	 * @param ctx the parse tree
	 */
	void enterIndex_statement(SQLParser.Index_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#index_statement}.
	 * @param ctx the parse tree
	 */
	void exitIndex_statement(SQLParser.Index_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#create_table_statement}.
	 * @param ctx the parse tree
	 */
	void enterCreate_table_statement(SQLParser.Create_table_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#create_table_statement}.
	 * @param ctx the parse tree
	 */
	void exitCreate_table_statement(SQLParser.Create_table_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_elements}.
	 * @param ctx the parse tree
	 */
	void enterTable_elements(SQLParser.Table_elementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_elements}.
	 * @param ctx the parse tree
	 */
	void exitTable_elements(SQLParser.Table_elementsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#field_element}.
	 * @param ctx the parse tree
	 */
	void enterField_element(SQLParser.Field_elementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#field_element}.
	 * @param ctx the parse tree
	 */
	void exitField_element(SQLParser.Field_elementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#field_type}.
	 * @param ctx the parse tree
	 */
	void enterField_type(SQLParser.Field_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#field_type}.
	 * @param ctx the parse tree
	 */
	void exitField_type(SQLParser.Field_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#param_clause}.
	 * @param ctx the parse tree
	 */
	void enterParam_clause(SQLParser.Param_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#param_clause}.
	 * @param ctx the parse tree
	 */
	void exitParam_clause(SQLParser.Param_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#param}.
	 * @param ctx the parse tree
	 */
	void enterParam(SQLParser.ParamContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#param}.
	 * @param ctx the parse tree
	 */
	void exitParam(SQLParser.ParamContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#method_specifier}.
	 * @param ctx the parse tree
	 */
	void enterMethod_specifier(SQLParser.Method_specifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#method_specifier}.
	 * @param ctx the parse tree
	 */
	void exitMethod_specifier(SQLParser.Method_specifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_space_specifier}.
	 * @param ctx the parse tree
	 */
	void enterTable_space_specifier(SQLParser.Table_space_specifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_space_specifier}.
	 * @param ctx the parse tree
	 */
	void exitTable_space_specifier(SQLParser.Table_space_specifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_space_name}.
	 * @param ctx the parse tree
	 */
	void enterTable_space_name(SQLParser.Table_space_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_space_name}.
	 * @param ctx the parse tree
	 */
	void exitTable_space_name(SQLParser.Table_space_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_partitioning_clauses}.
	 * @param ctx the parse tree
	 */
	void enterTable_partitioning_clauses(SQLParser.Table_partitioning_clausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_partitioning_clauses}.
	 * @param ctx the parse tree
	 */
	void exitTable_partitioning_clauses(SQLParser.Table_partitioning_clausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#range_partitions}.
	 * @param ctx the parse tree
	 */
	void enterRange_partitions(SQLParser.Range_partitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#range_partitions}.
	 * @param ctx the parse tree
	 */
	void exitRange_partitions(SQLParser.Range_partitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#range_value_clause_list}.
	 * @param ctx the parse tree
	 */
	void enterRange_value_clause_list(SQLParser.Range_value_clause_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#range_value_clause_list}.
	 * @param ctx the parse tree
	 */
	void exitRange_value_clause_list(SQLParser.Range_value_clause_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#range_value_clause}.
	 * @param ctx the parse tree
	 */
	void enterRange_value_clause(SQLParser.Range_value_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#range_value_clause}.
	 * @param ctx the parse tree
	 */
	void exitRange_value_clause(SQLParser.Range_value_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#hash_partitions}.
	 * @param ctx the parse tree
	 */
	void enterHash_partitions(SQLParser.Hash_partitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#hash_partitions}.
	 * @param ctx the parse tree
	 */
	void exitHash_partitions(SQLParser.Hash_partitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#individual_hash_partitions}.
	 * @param ctx the parse tree
	 */
	void enterIndividual_hash_partitions(SQLParser.Individual_hash_partitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#individual_hash_partitions}.
	 * @param ctx the parse tree
	 */
	void exitIndividual_hash_partitions(SQLParser.Individual_hash_partitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#individual_hash_partition}.
	 * @param ctx the parse tree
	 */
	void enterIndividual_hash_partition(SQLParser.Individual_hash_partitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#individual_hash_partition}.
	 * @param ctx the parse tree
	 */
	void exitIndividual_hash_partition(SQLParser.Individual_hash_partitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#hash_partitions_by_quantity}.
	 * @param ctx the parse tree
	 */
	void enterHash_partitions_by_quantity(SQLParser.Hash_partitions_by_quantityContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#hash_partitions_by_quantity}.
	 * @param ctx the parse tree
	 */
	void exitHash_partitions_by_quantity(SQLParser.Hash_partitions_by_quantityContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#list_partitions}.
	 * @param ctx the parse tree
	 */
	void enterList_partitions(SQLParser.List_partitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#list_partitions}.
	 * @param ctx the parse tree
	 */
	void exitList_partitions(SQLParser.List_partitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#list_value_clause_list}.
	 * @param ctx the parse tree
	 */
	void enterList_value_clause_list(SQLParser.List_value_clause_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#list_value_clause_list}.
	 * @param ctx the parse tree
	 */
	void exitList_value_clause_list(SQLParser.List_value_clause_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#list_value_partition}.
	 * @param ctx the parse tree
	 */
	void enterList_value_partition(SQLParser.List_value_partitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#list_value_partition}.
	 * @param ctx the parse tree
	 */
	void exitList_value_partition(SQLParser.List_value_partitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#column_partitions}.
	 * @param ctx the parse tree
	 */
	void enterColumn_partitions(SQLParser.Column_partitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#column_partitions}.
	 * @param ctx the parse tree
	 */
	void exitColumn_partitions(SQLParser.Column_partitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#partition_name}.
	 * @param ctx the parse tree
	 */
	void enterPartition_name(SQLParser.Partition_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#partition_name}.
	 * @param ctx the parse tree
	 */
	void exitPartition_name(SQLParser.Partition_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#drop_table_statement}.
	 * @param ctx the parse tree
	 */
	void enterDrop_table_statement(SQLParser.Drop_table_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#drop_table_statement}.
	 * @param ctx the parse tree
	 */
	void exitDrop_table_statement(SQLParser.Drop_table_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(SQLParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(SQLParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#nonreserved_keywords}.
	 * @param ctx the parse tree
	 */
	void enterNonreserved_keywords(SQLParser.Nonreserved_keywordsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#nonreserved_keywords}.
	 * @param ctx the parse tree
	 */
	void exitNonreserved_keywords(SQLParser.Nonreserved_keywordsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#unsigned_literal}.
	 * @param ctx the parse tree
	 */
	void enterUnsigned_literal(SQLParser.Unsigned_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#unsigned_literal}.
	 * @param ctx the parse tree
	 */
	void exitUnsigned_literal(SQLParser.Unsigned_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#general_literal}.
	 * @param ctx the parse tree
	 */
	void enterGeneral_literal(SQLParser.General_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#general_literal}.
	 * @param ctx the parse tree
	 */
	void exitGeneral_literal(SQLParser.General_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#datetime_literal}.
	 * @param ctx the parse tree
	 */
	void enterDatetime_literal(SQLParser.Datetime_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#datetime_literal}.
	 * @param ctx the parse tree
	 */
	void exitDatetime_literal(SQLParser.Datetime_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#time_literal}.
	 * @param ctx the parse tree
	 */
	void enterTime_literal(SQLParser.Time_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#time_literal}.
	 * @param ctx the parse tree
	 */
	void exitTime_literal(SQLParser.Time_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#timestamp_literal}.
	 * @param ctx the parse tree
	 */
	void enterTimestamp_literal(SQLParser.Timestamp_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#timestamp_literal}.
	 * @param ctx the parse tree
	 */
	void exitTimestamp_literal(SQLParser.Timestamp_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#date_literal}.
	 * @param ctx the parse tree
	 */
	void enterDate_literal(SQLParser.Date_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#date_literal}.
	 * @param ctx the parse tree
	 */
	void exitDate_literal(SQLParser.Date_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#boolean_literal}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_literal(SQLParser.Boolean_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#boolean_literal}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_literal(SQLParser.Boolean_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#data_type}.
	 * @param ctx the parse tree
	 */
	void enterData_type(SQLParser.Data_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#data_type}.
	 * @param ctx the parse tree
	 */
	void exitData_type(SQLParser.Data_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#predefined_type}.
	 * @param ctx the parse tree
	 */
	void enterPredefined_type(SQLParser.Predefined_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#predefined_type}.
	 * @param ctx the parse tree
	 */
	void exitPredefined_type(SQLParser.Predefined_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#network_type}.
	 * @param ctx the parse tree
	 */
	void enterNetwork_type(SQLParser.Network_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#network_type}.
	 * @param ctx the parse tree
	 */
	void exitNetwork_type(SQLParser.Network_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#character_string_type}.
	 * @param ctx the parse tree
	 */
	void enterCharacter_string_type(SQLParser.Character_string_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#character_string_type}.
	 * @param ctx the parse tree
	 */
	void exitCharacter_string_type(SQLParser.Character_string_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#type_length}.
	 * @param ctx the parse tree
	 */
	void enterType_length(SQLParser.Type_lengthContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#type_length}.
	 * @param ctx the parse tree
	 */
	void exitType_length(SQLParser.Type_lengthContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#national_character_string_type}.
	 * @param ctx the parse tree
	 */
	void enterNational_character_string_type(SQLParser.National_character_string_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#national_character_string_type}.
	 * @param ctx the parse tree
	 */
	void exitNational_character_string_type(SQLParser.National_character_string_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#binary_large_object_string_type}.
	 * @param ctx the parse tree
	 */
	void enterBinary_large_object_string_type(SQLParser.Binary_large_object_string_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#binary_large_object_string_type}.
	 * @param ctx the parse tree
	 */
	void exitBinary_large_object_string_type(SQLParser.Binary_large_object_string_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#numeric_type}.
	 * @param ctx the parse tree
	 */
	void enterNumeric_type(SQLParser.Numeric_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#numeric_type}.
	 * @param ctx the parse tree
	 */
	void exitNumeric_type(SQLParser.Numeric_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#exact_numeric_type}.
	 * @param ctx the parse tree
	 */
	void enterExact_numeric_type(SQLParser.Exact_numeric_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#exact_numeric_type}.
	 * @param ctx the parse tree
	 */
	void exitExact_numeric_type(SQLParser.Exact_numeric_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#approximate_numeric_type}.
	 * @param ctx the parse tree
	 */
	void enterApproximate_numeric_type(SQLParser.Approximate_numeric_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#approximate_numeric_type}.
	 * @param ctx the parse tree
	 */
	void exitApproximate_numeric_type(SQLParser.Approximate_numeric_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#precision_param}.
	 * @param ctx the parse tree
	 */
	void enterPrecision_param(SQLParser.Precision_paramContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#precision_param}.
	 * @param ctx the parse tree
	 */
	void exitPrecision_param(SQLParser.Precision_paramContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#boolean_type}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_type(SQLParser.Boolean_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#boolean_type}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_type(SQLParser.Boolean_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#datetime_type}.
	 * @param ctx the parse tree
	 */
	void enterDatetime_type(SQLParser.Datetime_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#datetime_type}.
	 * @param ctx the parse tree
	 */
	void exitDatetime_type(SQLParser.Datetime_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#bit_type}.
	 * @param ctx the parse tree
	 */
	void enterBit_type(SQLParser.Bit_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#bit_type}.
	 * @param ctx the parse tree
	 */
	void exitBit_type(SQLParser.Bit_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#binary_type}.
	 * @param ctx the parse tree
	 */
	void enterBinary_type(SQLParser.Binary_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#binary_type}.
	 * @param ctx the parse tree
	 */
	void exitBinary_type(SQLParser.Binary_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#value_expression_primary}.
	 * @param ctx the parse tree
	 */
	void enterValue_expression_primary(SQLParser.Value_expression_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#value_expression_primary}.
	 * @param ctx the parse tree
	 */
	void exitValue_expression_primary(SQLParser.Value_expression_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#parenthesized_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesized_value_expression(SQLParser.Parenthesized_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#parenthesized_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesized_value_expression(SQLParser.Parenthesized_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#nonparenthesized_value_expression_primary}.
	 * @param ctx the parse tree
	 */
	void enterNonparenthesized_value_expression_primary(SQLParser.Nonparenthesized_value_expression_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#nonparenthesized_value_expression_primary}.
	 * @param ctx the parse tree
	 */
	void exitNonparenthesized_value_expression_primary(SQLParser.Nonparenthesized_value_expression_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#unsigned_value_specification}.
	 * @param ctx the parse tree
	 */
	void enterUnsigned_value_specification(SQLParser.Unsigned_value_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#unsigned_value_specification}.
	 * @param ctx the parse tree
	 */
	void exitUnsigned_value_specification(SQLParser.Unsigned_value_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#unsigned_numeric_literal}.
	 * @param ctx the parse tree
	 */
	void enterUnsigned_numeric_literal(SQLParser.Unsigned_numeric_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#unsigned_numeric_literal}.
	 * @param ctx the parse tree
	 */
	void exitUnsigned_numeric_literal(SQLParser.Unsigned_numeric_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#signed_numerical_literal}.
	 * @param ctx the parse tree
	 */
	void enterSigned_numerical_literal(SQLParser.Signed_numerical_literalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#signed_numerical_literal}.
	 * @param ctx the parse tree
	 */
	void exitSigned_numerical_literal(SQLParser.Signed_numerical_literalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#set_function_specification}.
	 * @param ctx the parse tree
	 */
	void enterSet_function_specification(SQLParser.Set_function_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#set_function_specification}.
	 * @param ctx the parse tree
	 */
	void exitSet_function_specification(SQLParser.Set_function_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#aggregate_function}.
	 * @param ctx the parse tree
	 */
	void enterAggregate_function(SQLParser.Aggregate_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#aggregate_function}.
	 * @param ctx the parse tree
	 */
	void exitAggregate_function(SQLParser.Aggregate_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#general_set_function}.
	 * @param ctx the parse tree
	 */
	void enterGeneral_set_function(SQLParser.General_set_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#general_set_function}.
	 * @param ctx the parse tree
	 */
	void exitGeneral_set_function(SQLParser.General_set_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#set_function_type}.
	 * @param ctx the parse tree
	 */
	void enterSet_function_type(SQLParser.Set_function_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#set_function_type}.
	 * @param ctx the parse tree
	 */
	void exitSet_function_type(SQLParser.Set_function_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#filter_clause}.
	 * @param ctx the parse tree
	 */
	void enterFilter_clause(SQLParser.Filter_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#filter_clause}.
	 * @param ctx the parse tree
	 */
	void exitFilter_clause(SQLParser.Filter_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#grouping_operation}.
	 * @param ctx the parse tree
	 */
	void enterGrouping_operation(SQLParser.Grouping_operationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#grouping_operation}.
	 * @param ctx the parse tree
	 */
	void exitGrouping_operation(SQLParser.Grouping_operationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#case_expression}.
	 * @param ctx the parse tree
	 */
	void enterCase_expression(SQLParser.Case_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#case_expression}.
	 * @param ctx the parse tree
	 */
	void exitCase_expression(SQLParser.Case_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#case_abbreviation}.
	 * @param ctx the parse tree
	 */
	void enterCase_abbreviation(SQLParser.Case_abbreviationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#case_abbreviation}.
	 * @param ctx the parse tree
	 */
	void exitCase_abbreviation(SQLParser.Case_abbreviationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#case_specification}.
	 * @param ctx the parse tree
	 */
	void enterCase_specification(SQLParser.Case_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#case_specification}.
	 * @param ctx the parse tree
	 */
	void exitCase_specification(SQLParser.Case_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#simple_case}.
	 * @param ctx the parse tree
	 */
	void enterSimple_case(SQLParser.Simple_caseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#simple_case}.
	 * @param ctx the parse tree
	 */
	void exitSimple_case(SQLParser.Simple_caseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#searched_case}.
	 * @param ctx the parse tree
	 */
	void enterSearched_case(SQLParser.Searched_caseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#searched_case}.
	 * @param ctx the parse tree
	 */
	void exitSearched_case(SQLParser.Searched_caseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#simple_when_clause}.
	 * @param ctx the parse tree
	 */
	void enterSimple_when_clause(SQLParser.Simple_when_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#simple_when_clause}.
	 * @param ctx the parse tree
	 */
	void exitSimple_when_clause(SQLParser.Simple_when_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#searched_when_clause}.
	 * @param ctx the parse tree
	 */
	void enterSearched_when_clause(SQLParser.Searched_when_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#searched_when_clause}.
	 * @param ctx the parse tree
	 */
	void exitSearched_when_clause(SQLParser.Searched_when_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#else_clause}.
	 * @param ctx the parse tree
	 */
	void enterElse_clause(SQLParser.Else_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#else_clause}.
	 * @param ctx the parse tree
	 */
	void exitElse_clause(SQLParser.Else_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#result}.
	 * @param ctx the parse tree
	 */
	void enterResult(SQLParser.ResultContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#result}.
	 * @param ctx the parse tree
	 */
	void exitResult(SQLParser.ResultContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#cast_specification}.
	 * @param ctx the parse tree
	 */
	void enterCast_specification(SQLParser.Cast_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#cast_specification}.
	 * @param ctx the parse tree
	 */
	void exitCast_specification(SQLParser.Cast_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#cast_operand}.
	 * @param ctx the parse tree
	 */
	void enterCast_operand(SQLParser.Cast_operandContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#cast_operand}.
	 * @param ctx the parse tree
	 */
	void exitCast_operand(SQLParser.Cast_operandContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#cast_target}.
	 * @param ctx the parse tree
	 */
	void enterCast_target(SQLParser.Cast_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#cast_target}.
	 * @param ctx the parse tree
	 */
	void exitCast_target(SQLParser.Cast_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#value_expression}.
	 * @param ctx the parse tree
	 */
	void enterValue_expression(SQLParser.Value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#value_expression}.
	 * @param ctx the parse tree
	 */
	void exitValue_expression(SQLParser.Value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#common_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterCommon_value_expression(SQLParser.Common_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#common_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitCommon_value_expression(SQLParser.Common_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#numeric_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterNumeric_value_expression(SQLParser.Numeric_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#numeric_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitNumeric_value_expression(SQLParser.Numeric_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#term}.
	 * @param ctx the parse tree
	 */
	void enterTerm(SQLParser.TermContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#term}.
	 * @param ctx the parse tree
	 */
	void exitTerm(SQLParser.TermContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#factor}.
	 * @param ctx the parse tree
	 */
	void enterFactor(SQLParser.FactorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#factor}.
	 * @param ctx the parse tree
	 */
	void exitFactor(SQLParser.FactorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#array}.
	 * @param ctx the parse tree
	 */
	void enterArray(SQLParser.ArrayContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#array}.
	 * @param ctx the parse tree
	 */
	void exitArray(SQLParser.ArrayContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#numeric_primary}.
	 * @param ctx the parse tree
	 */
	void enterNumeric_primary(SQLParser.Numeric_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#numeric_primary}.
	 * @param ctx the parse tree
	 */
	void exitNumeric_primary(SQLParser.Numeric_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#sign}.
	 * @param ctx the parse tree
	 */
	void enterSign(SQLParser.SignContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#sign}.
	 * @param ctx the parse tree
	 */
	void exitSign(SQLParser.SignContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#numeric_value_function}.
	 * @param ctx the parse tree
	 */
	void enterNumeric_value_function(SQLParser.Numeric_value_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#numeric_value_function}.
	 * @param ctx the parse tree
	 */
	void exitNumeric_value_function(SQLParser.Numeric_value_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#extract_expression}.
	 * @param ctx the parse tree
	 */
	void enterExtract_expression(SQLParser.Extract_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#extract_expression}.
	 * @param ctx the parse tree
	 */
	void exitExtract_expression(SQLParser.Extract_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#extract_field}.
	 * @param ctx the parse tree
	 */
	void enterExtract_field(SQLParser.Extract_fieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#extract_field}.
	 * @param ctx the parse tree
	 */
	void exitExtract_field(SQLParser.Extract_fieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#time_zone_field}.
	 * @param ctx the parse tree
	 */
	void enterTime_zone_field(SQLParser.Time_zone_fieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#time_zone_field}.
	 * @param ctx the parse tree
	 */
	void exitTime_zone_field(SQLParser.Time_zone_fieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#extract_source}.
	 * @param ctx the parse tree
	 */
	void enterExtract_source(SQLParser.Extract_sourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#extract_source}.
	 * @param ctx the parse tree
	 */
	void exitExtract_source(SQLParser.Extract_sourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#string_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterString_value_expression(SQLParser.String_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#string_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitString_value_expression(SQLParser.String_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#character_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterCharacter_value_expression(SQLParser.Character_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#character_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitCharacter_value_expression(SQLParser.Character_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#character_factor}.
	 * @param ctx the parse tree
	 */
	void enterCharacter_factor(SQLParser.Character_factorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#character_factor}.
	 * @param ctx the parse tree
	 */
	void exitCharacter_factor(SQLParser.Character_factorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#character_primary}.
	 * @param ctx the parse tree
	 */
	void enterCharacter_primary(SQLParser.Character_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#character_primary}.
	 * @param ctx the parse tree
	 */
	void exitCharacter_primary(SQLParser.Character_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#string_value_function}.
	 * @param ctx the parse tree
	 */
	void enterString_value_function(SQLParser.String_value_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#string_value_function}.
	 * @param ctx the parse tree
	 */
	void exitString_value_function(SQLParser.String_value_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#trim_function}.
	 * @param ctx the parse tree
	 */
	void enterTrim_function(SQLParser.Trim_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#trim_function}.
	 * @param ctx the parse tree
	 */
	void exitTrim_function(SQLParser.Trim_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#trim_operands}.
	 * @param ctx the parse tree
	 */
	void enterTrim_operands(SQLParser.Trim_operandsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#trim_operands}.
	 * @param ctx the parse tree
	 */
	void exitTrim_operands(SQLParser.Trim_operandsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#trim_specification}.
	 * @param ctx the parse tree
	 */
	void enterTrim_specification(SQLParser.Trim_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#trim_specification}.
	 * @param ctx the parse tree
	 */
	void exitTrim_specification(SQLParser.Trim_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#boolean_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_value_expression(SQLParser.Boolean_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#boolean_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_value_expression(SQLParser.Boolean_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#or_predicate}.
	 * @param ctx the parse tree
	 */
	void enterOr_predicate(SQLParser.Or_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#or_predicate}.
	 * @param ctx the parse tree
	 */
	void exitOr_predicate(SQLParser.Or_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#and_predicate}.
	 * @param ctx the parse tree
	 */
	void enterAnd_predicate(SQLParser.And_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#and_predicate}.
	 * @param ctx the parse tree
	 */
	void exitAnd_predicate(SQLParser.And_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#boolean_factor}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_factor(SQLParser.Boolean_factorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#boolean_factor}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_factor(SQLParser.Boolean_factorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#boolean_test}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_test(SQLParser.Boolean_testContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#boolean_test}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_test(SQLParser.Boolean_testContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#is_clause}.
	 * @param ctx the parse tree
	 */
	void enterIs_clause(SQLParser.Is_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#is_clause}.
	 * @param ctx the parse tree
	 */
	void exitIs_clause(SQLParser.Is_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#truth_value}.
	 * @param ctx the parse tree
	 */
	void enterTruth_value(SQLParser.Truth_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#truth_value}.
	 * @param ctx the parse tree
	 */
	void exitTruth_value(SQLParser.Truth_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#boolean_primary}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_primary(SQLParser.Boolean_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#boolean_primary}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_primary(SQLParser.Boolean_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#boolean_predicand}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_predicand(SQLParser.Boolean_predicandContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#boolean_predicand}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_predicand(SQLParser.Boolean_predicandContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#parenthesized_boolean_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesized_boolean_value_expression(SQLParser.Parenthesized_boolean_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#parenthesized_boolean_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesized_boolean_value_expression(SQLParser.Parenthesized_boolean_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#row_value_expression}.
	 * @param ctx the parse tree
	 */
	void enterRow_value_expression(SQLParser.Row_value_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#row_value_expression}.
	 * @param ctx the parse tree
	 */
	void exitRow_value_expression(SQLParser.Row_value_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#row_value_special_case}.
	 * @param ctx the parse tree
	 */
	void enterRow_value_special_case(SQLParser.Row_value_special_caseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#row_value_special_case}.
	 * @param ctx the parse tree
	 */
	void exitRow_value_special_case(SQLParser.Row_value_special_caseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#explicit_row_value_constructor}.
	 * @param ctx the parse tree
	 */
	void enterExplicit_row_value_constructor(SQLParser.Explicit_row_value_constructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#explicit_row_value_constructor}.
	 * @param ctx the parse tree
	 */
	void exitExplicit_row_value_constructor(SQLParser.Explicit_row_value_constructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#row_value_predicand}.
	 * @param ctx the parse tree
	 */
	void enterRow_value_predicand(SQLParser.Row_value_predicandContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#row_value_predicand}.
	 * @param ctx the parse tree
	 */
	void exitRow_value_predicand(SQLParser.Row_value_predicandContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#row_value_constructor_predicand}.
	 * @param ctx the parse tree
	 */
	void enterRow_value_constructor_predicand(SQLParser.Row_value_constructor_predicandContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#row_value_constructor_predicand}.
	 * @param ctx the parse tree
	 */
	void exitRow_value_constructor_predicand(SQLParser.Row_value_constructor_predicandContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_expression}.
	 * @param ctx the parse tree
	 */
	void enterTable_expression(SQLParser.Table_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_expression}.
	 * @param ctx the parse tree
	 */
	void exitTable_expression(SQLParser.Table_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void enterFrom_clause(SQLParser.From_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void exitFrom_clause(SQLParser.From_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_reference_list}.
	 * @param ctx the parse tree
	 */
	void enterTable_reference_list(SQLParser.Table_reference_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_reference_list}.
	 * @param ctx the parse tree
	 */
	void exitTable_reference_list(SQLParser.Table_reference_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_reference}.
	 * @param ctx the parse tree
	 */
	void enterTable_reference(SQLParser.Table_referenceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_reference}.
	 * @param ctx the parse tree
	 */
	void exitTable_reference(SQLParser.Table_referenceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#joined_table}.
	 * @param ctx the parse tree
	 */
	void enterJoined_table(SQLParser.Joined_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#joined_table}.
	 * @param ctx the parse tree
	 */
	void exitJoined_table(SQLParser.Joined_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#joined_table_primary}.
	 * @param ctx the parse tree
	 */
	void enterJoined_table_primary(SQLParser.Joined_table_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#joined_table_primary}.
	 * @param ctx the parse tree
	 */
	void exitJoined_table_primary(SQLParser.Joined_table_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#cross_join}.
	 * @param ctx the parse tree
	 */
	void enterCross_join(SQLParser.Cross_joinContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#cross_join}.
	 * @param ctx the parse tree
	 */
	void exitCross_join(SQLParser.Cross_joinContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#qualified_join}.
	 * @param ctx the parse tree
	 */
	void enterQualified_join(SQLParser.Qualified_joinContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#qualified_join}.
	 * @param ctx the parse tree
	 */
	void exitQualified_join(SQLParser.Qualified_joinContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#natural_join}.
	 * @param ctx the parse tree
	 */
	void enterNatural_join(SQLParser.Natural_joinContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#natural_join}.
	 * @param ctx the parse tree
	 */
	void exitNatural_join(SQLParser.Natural_joinContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#union_join}.
	 * @param ctx the parse tree
	 */
	void enterUnion_join(SQLParser.Union_joinContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#union_join}.
	 * @param ctx the parse tree
	 */
	void exitUnion_join(SQLParser.Union_joinContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#join_type}.
	 * @param ctx the parse tree
	 */
	void enterJoin_type(SQLParser.Join_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#join_type}.
	 * @param ctx the parse tree
	 */
	void exitJoin_type(SQLParser.Join_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#outer_join_type}.
	 * @param ctx the parse tree
	 */
	void enterOuter_join_type(SQLParser.Outer_join_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#outer_join_type}.
	 * @param ctx the parse tree
	 */
	void exitOuter_join_type(SQLParser.Outer_join_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#outer_join_type_part2}.
	 * @param ctx the parse tree
	 */
	void enterOuter_join_type_part2(SQLParser.Outer_join_type_part2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#outer_join_type_part2}.
	 * @param ctx the parse tree
	 */
	void exitOuter_join_type_part2(SQLParser.Outer_join_type_part2Context ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#join_specification}.
	 * @param ctx the parse tree
	 */
	void enterJoin_specification(SQLParser.Join_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#join_specification}.
	 * @param ctx the parse tree
	 */
	void exitJoin_specification(SQLParser.Join_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#join_condition}.
	 * @param ctx the parse tree
	 */
	void enterJoin_condition(SQLParser.Join_conditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#join_condition}.
	 * @param ctx the parse tree
	 */
	void exitJoin_condition(SQLParser.Join_conditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#named_columns_join}.
	 * @param ctx the parse tree
	 */
	void enterNamed_columns_join(SQLParser.Named_columns_joinContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#named_columns_join}.
	 * @param ctx the parse tree
	 */
	void exitNamed_columns_join(SQLParser.Named_columns_joinContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_primary}.
	 * @param ctx the parse tree
	 */
	void enterTable_primary(SQLParser.Table_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_primary}.
	 * @param ctx the parse tree
	 */
	void exitTable_primary(SQLParser.Table_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#column_name_list}.
	 * @param ctx the parse tree
	 */
	void enterColumn_name_list(SQLParser.Column_name_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#column_name_list}.
	 * @param ctx the parse tree
	 */
	void exitColumn_name_list(SQLParser.Column_name_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#derived_table}.
	 * @param ctx the parse tree
	 */
	void enterDerived_table(SQLParser.Derived_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#derived_table}.
	 * @param ctx the parse tree
	 */
	void exitDerived_table(SQLParser.Derived_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#where_clause}.
	 * @param ctx the parse tree
	 */
	void enterWhere_clause(SQLParser.Where_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#where_clause}.
	 * @param ctx the parse tree
	 */
	void exitWhere_clause(SQLParser.Where_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#search_condition}.
	 * @param ctx the parse tree
	 */
	void enterSearch_condition(SQLParser.Search_conditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#search_condition}.
	 * @param ctx the parse tree
	 */
	void exitSearch_condition(SQLParser.Search_conditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#groupby_clause}.
	 * @param ctx the parse tree
	 */
	void enterGroupby_clause(SQLParser.Groupby_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#groupby_clause}.
	 * @param ctx the parse tree
	 */
	void exitGroupby_clause(SQLParser.Groupby_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#grouping_element_list}.
	 * @param ctx the parse tree
	 */
	void enterGrouping_element_list(SQLParser.Grouping_element_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#grouping_element_list}.
	 * @param ctx the parse tree
	 */
	void exitGrouping_element_list(SQLParser.Grouping_element_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#grouping_element}.
	 * @param ctx the parse tree
	 */
	void enterGrouping_element(SQLParser.Grouping_elementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#grouping_element}.
	 * @param ctx the parse tree
	 */
	void exitGrouping_element(SQLParser.Grouping_elementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#ordinary_grouping_set}.
	 * @param ctx the parse tree
	 */
	void enterOrdinary_grouping_set(SQLParser.Ordinary_grouping_setContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#ordinary_grouping_set}.
	 * @param ctx the parse tree
	 */
	void exitOrdinary_grouping_set(SQLParser.Ordinary_grouping_setContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#ordinary_grouping_set_list}.
	 * @param ctx the parse tree
	 */
	void enterOrdinary_grouping_set_list(SQLParser.Ordinary_grouping_set_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#ordinary_grouping_set_list}.
	 * @param ctx the parse tree
	 */
	void exitOrdinary_grouping_set_list(SQLParser.Ordinary_grouping_set_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#rollup_list}.
	 * @param ctx the parse tree
	 */
	void enterRollup_list(SQLParser.Rollup_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#rollup_list}.
	 * @param ctx the parse tree
	 */
	void exitRollup_list(SQLParser.Rollup_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#cube_list}.
	 * @param ctx the parse tree
	 */
	void enterCube_list(SQLParser.Cube_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#cube_list}.
	 * @param ctx the parse tree
	 */
	void exitCube_list(SQLParser.Cube_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#empty_grouping_set}.
	 * @param ctx the parse tree
	 */
	void enterEmpty_grouping_set(SQLParser.Empty_grouping_setContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#empty_grouping_set}.
	 * @param ctx the parse tree
	 */
	void exitEmpty_grouping_set(SQLParser.Empty_grouping_setContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#having_clause}.
	 * @param ctx the parse tree
	 */
	void enterHaving_clause(SQLParser.Having_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#having_clause}.
	 * @param ctx the parse tree
	 */
	void exitHaving_clause(SQLParser.Having_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#row_value_predicand_list}.
	 * @param ctx the parse tree
	 */
	void enterRow_value_predicand_list(SQLParser.Row_value_predicand_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#row_value_predicand_list}.
	 * @param ctx the parse tree
	 */
	void exitRow_value_predicand_list(SQLParser.Row_value_predicand_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#query_expression}.
	 * @param ctx the parse tree
	 */
	void enterQuery_expression(SQLParser.Query_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#query_expression}.
	 * @param ctx the parse tree
	 */
	void exitQuery_expression(SQLParser.Query_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#query_expression_body}.
	 * @param ctx the parse tree
	 */
	void enterQuery_expression_body(SQLParser.Query_expression_bodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#query_expression_body}.
	 * @param ctx the parse tree
	 */
	void exitQuery_expression_body(SQLParser.Query_expression_bodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#non_join_query_expression}.
	 * @param ctx the parse tree
	 */
	void enterNon_join_query_expression(SQLParser.Non_join_query_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#non_join_query_expression}.
	 * @param ctx the parse tree
	 */
	void exitNon_join_query_expression(SQLParser.Non_join_query_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#query_term}.
	 * @param ctx the parse tree
	 */
	void enterQuery_term(SQLParser.Query_termContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#query_term}.
	 * @param ctx the parse tree
	 */
	void exitQuery_term(SQLParser.Query_termContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#non_join_query_term}.
	 * @param ctx the parse tree
	 */
	void enterNon_join_query_term(SQLParser.Non_join_query_termContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#non_join_query_term}.
	 * @param ctx the parse tree
	 */
	void exitNon_join_query_term(SQLParser.Non_join_query_termContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#query_primary}.
	 * @param ctx the parse tree
	 */
	void enterQuery_primary(SQLParser.Query_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#query_primary}.
	 * @param ctx the parse tree
	 */
	void exitQuery_primary(SQLParser.Query_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#non_join_query_primary}.
	 * @param ctx the parse tree
	 */
	void enterNon_join_query_primary(SQLParser.Non_join_query_primaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#non_join_query_primary}.
	 * @param ctx the parse tree
	 */
	void exitNon_join_query_primary(SQLParser.Non_join_query_primaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#simple_table}.
	 * @param ctx the parse tree
	 */
	void enterSimple_table(SQLParser.Simple_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#simple_table}.
	 * @param ctx the parse tree
	 */
	void exitSimple_table(SQLParser.Simple_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#explicit_table}.
	 * @param ctx the parse tree
	 */
	void enterExplicit_table(SQLParser.Explicit_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#explicit_table}.
	 * @param ctx the parse tree
	 */
	void exitExplicit_table(SQLParser.Explicit_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_or_query_name}.
	 * @param ctx the parse tree
	 */
	void enterTable_or_query_name(SQLParser.Table_or_query_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_or_query_name}.
	 * @param ctx the parse tree
	 */
	void exitTable_or_query_name(SQLParser.Table_or_query_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_name}.
	 * @param ctx the parse tree
	 */
	void enterTable_name(SQLParser.Table_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_name}.
	 * @param ctx the parse tree
	 */
	void exitTable_name(SQLParser.Table_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#query_specification}.
	 * @param ctx the parse tree
	 */
	void enterQuery_specification(SQLParser.Query_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#query_specification}.
	 * @param ctx the parse tree
	 */
	void exitQuery_specification(SQLParser.Query_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#select_list}.
	 * @param ctx the parse tree
	 */
	void enterSelect_list(SQLParser.Select_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#select_list}.
	 * @param ctx the parse tree
	 */
	void exitSelect_list(SQLParser.Select_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#select_sublist}.
	 * @param ctx the parse tree
	 */
	void enterSelect_sublist(SQLParser.Select_sublistContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#select_sublist}.
	 * @param ctx the parse tree
	 */
	void exitSelect_sublist(SQLParser.Select_sublistContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#derived_column}.
	 * @param ctx the parse tree
	 */
	void enterDerived_column(SQLParser.Derived_columnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#derived_column}.
	 * @param ctx the parse tree
	 */
	void exitDerived_column(SQLParser.Derived_columnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#qualified_asterisk}.
	 * @param ctx the parse tree
	 */
	void enterQualified_asterisk(SQLParser.Qualified_asteriskContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#qualified_asterisk}.
	 * @param ctx the parse tree
	 */
	void exitQualified_asterisk(SQLParser.Qualified_asteriskContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#set_qualifier}.
	 * @param ctx the parse tree
	 */
	void enterSet_qualifier(SQLParser.Set_qualifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#set_qualifier}.
	 * @param ctx the parse tree
	 */
	void exitSet_qualifier(SQLParser.Set_qualifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#column_reference}.
	 * @param ctx the parse tree
	 */
	void enterColumn_reference(SQLParser.Column_referenceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#column_reference}.
	 * @param ctx the parse tree
	 */
	void exitColumn_reference(SQLParser.Column_referenceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#as_clause}.
	 * @param ctx the parse tree
	 */
	void enterAs_clause(SQLParser.As_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#as_clause}.
	 * @param ctx the parse tree
	 */
	void exitAs_clause(SQLParser.As_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#column_reference_list}.
	 * @param ctx the parse tree
	 */
	void enterColumn_reference_list(SQLParser.Column_reference_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#column_reference_list}.
	 * @param ctx the parse tree
	 */
	void exitColumn_reference_list(SQLParser.Column_reference_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#scalar_subquery}.
	 * @param ctx the parse tree
	 */
	void enterScalar_subquery(SQLParser.Scalar_subqueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#scalar_subquery}.
	 * @param ctx the parse tree
	 */
	void exitScalar_subquery(SQLParser.Scalar_subqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#row_subquery}.
	 * @param ctx the parse tree
	 */
	void enterRow_subquery(SQLParser.Row_subqueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#row_subquery}.
	 * @param ctx the parse tree
	 */
	void exitRow_subquery(SQLParser.Row_subqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#table_subquery}.
	 * @param ctx the parse tree
	 */
	void enterTable_subquery(SQLParser.Table_subqueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#table_subquery}.
	 * @param ctx the parse tree
	 */
	void exitTable_subquery(SQLParser.Table_subqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#subquery}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(SQLParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#subquery}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(SQLParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(SQLParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(SQLParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#comparison_predicate}.
	 * @param ctx the parse tree
	 */
	void enterComparison_predicate(SQLParser.Comparison_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#comparison_predicate}.
	 * @param ctx the parse tree
	 */
	void exitComparison_predicate(SQLParser.Comparison_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#comp_op}.
	 * @param ctx the parse tree
	 */
	void enterComp_op(SQLParser.Comp_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#comp_op}.
	 * @param ctx the parse tree
	 */
	void exitComp_op(SQLParser.Comp_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#between_predicate}.
	 * @param ctx the parse tree
	 */
	void enterBetween_predicate(SQLParser.Between_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#between_predicate}.
	 * @param ctx the parse tree
	 */
	void exitBetween_predicate(SQLParser.Between_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#between_predicate_part_2}.
	 * @param ctx the parse tree
	 */
	void enterBetween_predicate_part_2(SQLParser.Between_predicate_part_2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#between_predicate_part_2}.
	 * @param ctx the parse tree
	 */
	void exitBetween_predicate_part_2(SQLParser.Between_predicate_part_2Context ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#in_predicate}.
	 * @param ctx the parse tree
	 */
	void enterIn_predicate(SQLParser.In_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#in_predicate}.
	 * @param ctx the parse tree
	 */
	void exitIn_predicate(SQLParser.In_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#in_predicate_value}.
	 * @param ctx the parse tree
	 */
	void enterIn_predicate_value(SQLParser.In_predicate_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#in_predicate_value}.
	 * @param ctx the parse tree
	 */
	void exitIn_predicate_value(SQLParser.In_predicate_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#in_value_list}.
	 * @param ctx the parse tree
	 */
	void enterIn_value_list(SQLParser.In_value_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#in_value_list}.
	 * @param ctx the parse tree
	 */
	void exitIn_value_list(SQLParser.In_value_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#pattern_matching_predicate}.
	 * @param ctx the parse tree
	 */
	void enterPattern_matching_predicate(SQLParser.Pattern_matching_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#pattern_matching_predicate}.
	 * @param ctx the parse tree
	 */
	void exitPattern_matching_predicate(SQLParser.Pattern_matching_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#pattern_matcher}.
	 * @param ctx the parse tree
	 */
	void enterPattern_matcher(SQLParser.Pattern_matcherContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#pattern_matcher}.
	 * @param ctx the parse tree
	 */
	void exitPattern_matcher(SQLParser.Pattern_matcherContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#negativable_matcher}.
	 * @param ctx the parse tree
	 */
	void enterNegativable_matcher(SQLParser.Negativable_matcherContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#negativable_matcher}.
	 * @param ctx the parse tree
	 */
	void exitNegativable_matcher(SQLParser.Negativable_matcherContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#regex_matcher}.
	 * @param ctx the parse tree
	 */
	void enterRegex_matcher(SQLParser.Regex_matcherContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#regex_matcher}.
	 * @param ctx the parse tree
	 */
	void exitRegex_matcher(SQLParser.Regex_matcherContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#null_predicate}.
	 * @param ctx the parse tree
	 */
	void enterNull_predicate(SQLParser.Null_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#null_predicate}.
	 * @param ctx the parse tree
	 */
	void exitNull_predicate(SQLParser.Null_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#quantified_comparison_predicate}.
	 * @param ctx the parse tree
	 */
	void enterQuantified_comparison_predicate(SQLParser.Quantified_comparison_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#quantified_comparison_predicate}.
	 * @param ctx the parse tree
	 */
	void exitQuantified_comparison_predicate(SQLParser.Quantified_comparison_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#quantifier}.
	 * @param ctx the parse tree
	 */
	void enterQuantifier(SQLParser.QuantifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#quantifier}.
	 * @param ctx the parse tree
	 */
	void exitQuantifier(SQLParser.QuantifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#all}.
	 * @param ctx the parse tree
	 */
	void enterAll(SQLParser.AllContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#all}.
	 * @param ctx the parse tree
	 */
	void exitAll(SQLParser.AllContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#some}.
	 * @param ctx the parse tree
	 */
	void enterSome(SQLParser.SomeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#some}.
	 * @param ctx the parse tree
	 */
	void exitSome(SQLParser.SomeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#exists_predicate}.
	 * @param ctx the parse tree
	 */
	void enterExists_predicate(SQLParser.Exists_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#exists_predicate}.
	 * @param ctx the parse tree
	 */
	void exitExists_predicate(SQLParser.Exists_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#unique_predicate}.
	 * @param ctx the parse tree
	 */
	void enterUnique_predicate(SQLParser.Unique_predicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#unique_predicate}.
	 * @param ctx the parse tree
	 */
	void exitUnique_predicate(SQLParser.Unique_predicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#primary_datetime_field}.
	 * @param ctx the parse tree
	 */
	void enterPrimary_datetime_field(SQLParser.Primary_datetime_fieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#primary_datetime_field}.
	 * @param ctx the parse tree
	 */
	void exitPrimary_datetime_field(SQLParser.Primary_datetime_fieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#non_second_primary_datetime_field}.
	 * @param ctx the parse tree
	 */
	void enterNon_second_primary_datetime_field(SQLParser.Non_second_primary_datetime_fieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#non_second_primary_datetime_field}.
	 * @param ctx the parse tree
	 */
	void exitNon_second_primary_datetime_field(SQLParser.Non_second_primary_datetime_fieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#extended_datetime_field}.
	 * @param ctx the parse tree
	 */
	void enterExtended_datetime_field(SQLParser.Extended_datetime_fieldContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#extended_datetime_field}.
	 * @param ctx the parse tree
	 */
	void exitExtended_datetime_field(SQLParser.Extended_datetime_fieldContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#routine_invocation}.
	 * @param ctx the parse tree
	 */
	void enterRoutine_invocation(SQLParser.Routine_invocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#routine_invocation}.
	 * @param ctx the parse tree
	 */
	void exitRoutine_invocation(SQLParser.Routine_invocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#function_names_for_reserved_words}.
	 * @param ctx the parse tree
	 */
	void enterFunction_names_for_reserved_words(SQLParser.Function_names_for_reserved_wordsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#function_names_for_reserved_words}.
	 * @param ctx the parse tree
	 */
	void exitFunction_names_for_reserved_words(SQLParser.Function_names_for_reserved_wordsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#function_name}.
	 * @param ctx the parse tree
	 */
	void enterFunction_name(SQLParser.Function_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#function_name}.
	 * @param ctx the parse tree
	 */
	void exitFunction_name(SQLParser.Function_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#sql_argument_list}.
	 * @param ctx the parse tree
	 */
	void enterSql_argument_list(SQLParser.Sql_argument_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#sql_argument_list}.
	 * @param ctx the parse tree
	 */
	void exitSql_argument_list(SQLParser.Sql_argument_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#orderby_clause}.
	 * @param ctx the parse tree
	 */
	void enterOrderby_clause(SQLParser.Orderby_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#orderby_clause}.
	 * @param ctx the parse tree
	 */
	void exitOrderby_clause(SQLParser.Orderby_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#sort_specifier_list}.
	 * @param ctx the parse tree
	 */
	void enterSort_specifier_list(SQLParser.Sort_specifier_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#sort_specifier_list}.
	 * @param ctx the parse tree
	 */
	void exitSort_specifier_list(SQLParser.Sort_specifier_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#sort_specifier}.
	 * @param ctx the parse tree
	 */
	void enterSort_specifier(SQLParser.Sort_specifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#sort_specifier}.
	 * @param ctx the parse tree
	 */
	void exitSort_specifier(SQLParser.Sort_specifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#order_specification}.
	 * @param ctx the parse tree
	 */
	void enterOrder_specification(SQLParser.Order_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#order_specification}.
	 * @param ctx the parse tree
	 */
	void exitOrder_specification(SQLParser.Order_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void enterLimit_clause(SQLParser.Limit_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void exitLimit_clause(SQLParser.Limit_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#null_ordering}.
	 * @param ctx the parse tree
	 */
	void enterNull_ordering(SQLParser.Null_orderingContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#null_ordering}.
	 * @param ctx the parse tree
	 */
	void exitNull_ordering(SQLParser.Null_orderingContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#insert_statement}.
	 * @param ctx the parse tree
	 */
	void enterInsert_statement(SQLParser.Insert_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#insert_statement}.
	 * @param ctx the parse tree
	 */
	void exitInsert_statement(SQLParser.Insert_statementContext ctx);
}
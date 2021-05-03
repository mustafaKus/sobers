import os
from argparse import ArgumentParser

from statement_unifier import Bank1StatementTransformer, Bank2StatementTransformer, Bank3StatementTransformer, \
    StatementsUnifier

if __name__ == "__main__":

    parser = ArgumentParser("lists the arguments")
    parser.add_argument("-d", "--directory-path", default=os.getcwd(), help="output directory path")
    parser.add_argument("-o", "--output-file-name", default="unified_statement.csv", help="output file name")
    args = parser.parse_args()

    bank_1_statement = Bank1StatementTransformer(
        "https://gist.githubusercontent.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/b005c3dd94090d656ca2b00d7c2ac50ddeb3d88c/bank1.csv")
    bank_2_statement = Bank2StatementTransformer(
        "https://gist.githubusercontent.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/b005c3dd94090d656ca2b00d7c2ac50ddeb3d88c/bank2.csv")
    bank_3_statement = Bank3StatementTransformer(
        "https://gist.githubusercontent.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/b005c3dd94090d656ca2b00d7c2ac50ddeb3d88c/bank3.csv")

    statements_unifier = StatementsUnifier(args.directory_path, args.output_file_name)

    for statement in [bank_1_statement, bank_2_statement, bank_3_statement]:
        statements_unifier.add_statement(statement.df())
    statements_unifier.write()

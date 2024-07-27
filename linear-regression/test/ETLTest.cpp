#include <iostream>
#include "../ETL/ETL.h"

int main()
{
  std::string filename = "../data/housing_data.csv";

  ETL ETL;

  ETL::Dataset data = ETL.readCSV(filename);

  std::cout << "Printing row" << std::endl;

  ETL.printRow(data[2]);

  std::cout << "Dropping rows without nulls" << std::endl;

  ETL.dropRowsWithNulls();

  ETL::Dataset transposedData = ETL.transposeDataset(data);

  std::cout << "Transposed row" << std::endl;

  ETL.printRow(transposedData[2]);

  std::cout << "Encoding dataset" << std::endl;

  ETL::Dataset encodedData = ETL.encodeDataset();

  ETL.printRow(encodedData[2]);


  ETL.saveToCSV(encodedData, "encoded_housing_data.csv");


  return 0;
}
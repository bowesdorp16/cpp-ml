#ifndef ETL_H
#define ETL_H

#include <vector>
#include <string>
#include <iostream>

class ETL
{
public:
  using Row = std::vector<std::string>;
  using Dataset = std::vector<Row>;

  ETL() {}

  Row getHeader() const
  {
    return header;
  }

  Dataset getDataset() const
  {
    return dataset;
  }

  void printHeader()
  {
    std::string headerString = convertRowToString(header);
    std::cout << headerString << std::endl;
  }

  void printRow(const Row &row)
  {
    std::string rowString = convertRowToString(row);
    std::cout << rowString << std::endl;
  }

  Dataset readCSV(const std::string &filename, bool header = true);

  Dataset dropRowsWithNulls();

  Dataset encodeDataset();

  Dataset transposeDataset(const Dataset &dataset);

private:
  Row header;

  Dataset dataset;

  void setHeader(const Row &header)
  {
    this->header = header;
  }

  void setDataset(const Dataset &dataset)
  {
    this->dataset = dataset;
  }

  std::string convertRowToString(const Row &row);

  Row parseLine(const std::string &line);

  bool validateRow(const Row &row) const;

  bool rowOnlyContainsNumbers(const Row &row) const;
};

#endif
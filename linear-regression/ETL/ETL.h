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

  using EncodedRow = std::vector<double>;
  using EncodedDataset = std::vector<EncodedRow>;

  ETL() {}

  Row getHeader() const
  {
    return header;
  }

  Dataset getDataset() const
  {
    return dataset;
  }

  EncodedDataset getEncodedDataset() const
  {
    return encodedDataset;
  }

  void printHeader()
  {
    std::string headerString = convertRowToString(header);
    std::cout << headerString << std::endl;
  }

  void printRow(const Row &row);

  void saveToCSV(const Dataset &dataset, const std::string &filename);

  Dataset readCSV(const std::string &filename, bool header = true);

  Dataset dropRowsWithNulls();

  Dataset transposeDataset(const Dataset &dataset);

  Dataset encodeDataset();



private:
  Row header;

  Dataset dataset;

  EncodedDataset encodedDataset;


  void setHeader(const Row &header)
  {
    this->header = header;
  }

  void setDataset(const Dataset &dataset)
  {
    this->dataset = dataset;
  }

  void setEncodedDataset(const EncodedDataset &encodedDataset)
  {
    this->encodedDataset = encodedDataset;
  }

  std::string convertRowToString(const Row &row);

  Row parseLine(const std::string &line);

  bool validateRow(const Row &row) const;

  bool rowOnlyContainsNumbers(const Row &row) const;
};

#endif
#include "ETL.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

ETL::Dataset ETL::readCSV(const std::string &filename, bool header)
{
  Dataset dataset;
  std::ifstream file(filename);

  if (!file.is_open())
  {
    throw std::runtime_error("Could not open file");
  }

  std::string line;
  ETL::Row row;
  while (std::getline(file, line))
  {
    row = parseLine(line);
    dataset.push_back(row);
  }

  if (header)
  {
    setHeader(dataset[0]);

    dataset.erase(dataset.begin());
  }

  setDataset(dataset);

  return dataset;
}

ETL::Row ETL::parseLine(const std::string &line)
{
  std::istringstream string_stream(line);
  std::string cell;
  Row row;

  while (std::getline(string_stream, cell, ','))
  {
    row.push_back(cell);
  }

  return row;
}

std::string ETL::convertRowToString(const Row &row)
{
  std::string str;
  for (const auto &cell : row)
  {
    str += cell + ",";
  }

  str.pop_back();

  return str;
}

bool ETL::validateRow(const Row &row) const
{
  for (const auto &cell : row)
  {
    if (cell.empty())
    {
      std::cerr << "Validation failed: Empty cell found in row" << std::endl;
      return false;
    }
  }
  // Row is valid
  return true;
}

ETL::Dataset ETL::dropRowsWithNulls()
{
  Dataset datasetWithoutNulls;

  Dataset dataset = getDataset();

  for (const auto &row : dataset)
  {
    if (validateRow(row))
    {
      datasetWithoutNulls.push_back(row);
    }
  }

  setDataset(datasetWithoutNulls);

  std::cout << "Dropped " << dataset.size() - datasetWithoutNulls.size() << " rows with nulls" << std::endl;

  return datasetWithoutNulls;
}

ETL::Dataset ETL::transposeDataset(const Dataset &dataset)
{

  if (dataset.empty())
    return dataset;

  size_t rows = dataset.size();
  size_t cols = dataset[0].size();

  Dataset transposed(cols, std::vector<std::string>(rows));

  for (size_t i = 0; i < rows; ++i)
  {
    for (size_t j = 0; j < cols; ++j)
    {
      transposed[j][i] = dataset[i][j];
    }
  }

  return transposed;
}

bool ETL::rowOnlyContainsNumbers(const Row &row) const
{
  for (const auto &cell : row)
  {
    if (!std::all_of(cell.begin(), cell.end(), ::isdigit))
    {
      return false;
    }
  }

  return true;
}

ETL::Dataset ETL::encodeDataset()
{
  Dataset dataset = getDataset();

  Dataset transposed = transposeDataset(dataset);

  for (auto &row : transposed)
  {
    if (!rowOnlyContainsNumbers(row))
    {
      std::cout << "Row does not only contain numbers: " << convertRowToString(row) << std::endl;
    }
  }


  setDataset(dataset);

  return dataset;
}

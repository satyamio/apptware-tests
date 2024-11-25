package com.apptware.interview.stream.impl;

import com.apptware.interview.stream.DataReader;
import com.apptware.interview.stream.PaginationService;
import jakarta.annotation.Nonnull;

import java.util.stream.Stream;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

//import org.glassfish.jaxb.runtime.v2.schemagen.xmlschema.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
class DataReaderImpl implements DataReader {

  @Autowired
  private PaginationService paginationService;

  @Override
  public Stream<String> fetchLimitadData(int limit) {
    return fetchPaginatedDataAsStream().limit(limit);
  }

  @Override
  public Stream<String> fetchFullData() {
    return fetchPaginatedDataAsStream();
  }

  /**
   * This method is where the candidate should add the implementation. Logs have
   * been added to track
   * the data fetching behavior. Do not modify any other areas of the code.
   */
  private @Nonnull Stream<String> fetchPaginatedDataAsStream() {
    log.info("Fetching paginated data as a stream.");

    int pageSize = 100; // Number of items per page
    int totalPages = (int) Math.ceil((double) PaginationService.FULL_DATA_SIZE / pageSize); // Calculate total pages
                                                                                            // based on FULL_DATA_SIZE

    // Use Stream.iterate() to generate a stream of pages (1-based index) and
    // flatMap to the data items
    return Stream.iterate(1, page -> page <= totalPages, page -> page + 1)
        .flatMap(page -> {
          log.info("Fetching data for page {}", page);
          List<String> pageData = paginationService.getPaginatedData(page, pageSize); // Get data for the current page
          log.info("Page {} contains {} items", page, pageData.size());
          return pageData.stream(); // Convert page data to a stream of items
        })
        .peek(item -> log.info("Fetched Item: {}", item)); // Log each item fetched
  }

}

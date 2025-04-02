// app/static/js/main.js
let allMovieData = []; // Store all movie data here

async function fetchDataAndVisualize(formData) {
  try {
    const response = await fetch("/submit-source-a", {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    console.log("Data received:", data);
    const taskId = data.task_id;

    // Poll for task completion and fetch data
    await pollForTaskCompletion(taskId);
  } catch (error) {
    console.error("Error fetching or visualizing data:", error);
    // Optionally, display an error message to the user
  }
}

async function pollForTaskCompletion(taskId) {
  let taskComplete = false;
  while (!taskComplete) {
    const taskStatus = await getTaskStatus(taskId);
    console.log(`Task ${taskId} status: ${taskStatus}`);

    if (taskStatus === "completed") {
      taskComplete = true;
      const movieData = await getTaskData(taskId);
      allMovieData = movieData; // Store all data
      populateGenreFilter(movieData); // Populate the genre filter
      visualizeData(movieData); // Visualize all data initially
    } else if (taskStatus === "failed") {
      console.error(`Task ${taskId} failed.`);
      return;
    } else {
      await new Promise((resolve) => setTimeout(resolve, 2000)); // Wait for 2 seconds
    }
  }
}

async function getTaskStatus(taskId) {
  try {
    const response = await fetch(`/api/tasks/${taskId}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    return data.status;
  } catch (error) {
    console.error(`Error getting task ${taskId} status:`, error);
    return "failed"; // Treat errors as failed
  }
}

async function getTaskData(taskId) {
  try {
    const response = await fetch(`/api/tasks/${taskId}/data`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    return data;
  } catch (error) {
    console.error(`Error getting data for task ${taskId}:`, error);
    return []; // Return empty array on error
  }
}

function visualizeData(data) {
  // Clear previous charts
  d3.select("#time-series-chart").selectAll("*").remove();

  // Time Series Chart
  const timeSeriesData = prepareTimeSeriesData(data);
  createTimeSeriesChart(timeSeriesData);
}

function prepareTimeSeriesData(data) {
  const yearCounts = {};
  data.forEach((d) => {
    const year = new Date(d.release_date).getFullYear();
    yearCounts[year] = (yearCounts[year] || 0) + 1;
  });

  const lastFiveYears = Object.keys(yearCounts)
    .map(Number)
    .sort((a, b) => b - a)
    .slice(0, 5);

  const timeSeriesData = lastFiveYears.map((year) => ({
    year: year,
    count: yearCounts[year] || 0,
  }));
  return timeSeriesData;
}

function createTimeSeriesChart(data) {
  const margin = { top: 20, right: 20, bottom: 30, left: 50 };
  const width = 600 - margin.left - margin.right;
  const height = 400 - margin.top - margin.bottom;

  const svg = d3
    .select("#time-series-chart")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", `translate(${margin.left},${margin.top})`);

  const x = d3
    .scaleBand()
    .domain(data.map((d) => d.year))
    .range([0, width])
    .padding(0.1);

  const y = d3
    .scaleLinear()
    .domain([0, d3.max(data, (d) => d.count)])
    .range([height, 0]);

  svg
    .append("g")
    .attr("transform", `translate(0,${height})`)
    .call(d3.axisBottom(x));

  svg.append("g").call(d3.axisLeft(y));

  svg
    .selectAll(".bar")
    .data(data)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("x", (d) => x(d.year))
    .attr("y", (d) => y(d.count))
    .attr("width", x.bandwidth())
    .attr("height", (d) => height - y(d.count));
}

function populateGenreFilter(data) {
    const genreFilter = document.getElementById("genre-filter");
    const allGenres = new Set();
    data.forEach(movie => {
        if (movie.genres) {
            movie.genres.forEach(genre => allGenres.add(genre));
        }
    });

    allGenres.forEach(genre => {
        const option = document.createElement("option");
        option.value = genre;
        option.text = genre;
        genreFilter.appendChild(option);
    });
}

function filterByGenre(genre) {
    if (genre === "all") {
        visualizeData(allMovieData);
    } else {
        const filteredData = allMovieData.filter(movie => movie.genres && movie.genres.includes(genre));
        visualizeData(filteredData);
    }
}

// Attach event listener to the form
document.addEventListener("DOMContentLoaded", function () {
  const form = document.getElementById("source-a-form");
  form.addEventListener("submit", function (event) {
    event.preventDefault(); // Prevent default form submission
    const formData = new FormData(form);
    fetchDataAndVisualize(formData);
  });

    // Attach event listener to the genre filter
    const genreFilter = document.getElementById("genre-filter");
    genreFilter.addEventListener("change", function () {
        const selectedGenre = this.value;
        filterByGenre(selectedGenre);
    });
});

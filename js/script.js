const margin = {top: 50, right: 50, bottom: 50, left: 50};

const svg = d3.select("svg");
const width = +svg.attr("width") - margin.left - margin.right;
const height = +svg.attr("height") - margin.top - margin.bottom;

const vis = svg.append("g")
.attr("transform", `translate(${margin.left},${margin.top})`);

const rect = vis.append("rect")
.attr("class", "content")
.attr("width", width)
.attr("height", height);

const xScale = d3.scaleLinear()
.domain([margin.left, margin.left + width])
.range([0, width]);

const yScale = d3.scaleLinear()
.domain([margin.top, margin.top + height])
.range([0, height]);
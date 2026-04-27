## Maritime Tanker Tracker

A real-time AIS data pipeline for detecting cargo-handling events and estimating vessel payload, implemented in R across four concurrent processes.

#### Overview

Ingests live global AIS data, isolates tanker vessels, cleans the stream, and detects when and where cargo is loaded or unloaded — along with an estimated mass transferred in metric tonnes and barrels.

#### Pipeline

| Process | Role |
|---|---|
| **P1 – Ingestion** | Connects to AISStream.io via WebSocket; buffers raw hex messages to disk. |
| **P2 – Parser** | Pre-filters at hex level (no full deserialisation), parses tanker messages, deduplicates, writes RDS files. |
| **P3 – Analysis** | Cleaning → draught-change detection -> GMM berthing confirmation -> payload estimation (24h cycle). |
| **P4 – Storage** | Writes P2 output to MySQL for persistence. |
| **Background Launcher** | Runs each process, deals with WiFi connectivity, emergency shutdowns, and hung processes. | 

#### Methods

- **Tanker identification:** AIS ship type codes 80–89, matched at the hexadecimal level before parsing.
- **Cleaning:** Deterministic bounds filtering + per-vessel DBSCAN on draught (ε = 2 m, minPts = 2).
- **Event detection:** Draught change > 1 m, elapsed time > 12 h, distance > 12 km (Zhang et al., 2025).
- **Berthing confirmation:** Two-component GMM fitted to speed-over-ground; threshold = moored mean + 3σ.
- **DWT estimation:** Author-derived OLS regression: `DWT = −971.23 + 1.03 × (L × B × d)`.
- **Cargo mass:** TPCI × draught change, corrected for ballast (MARPOL light ballast formula + 5% DWT heavy ballast).
- **Unit conversion:**Metric tonnes → barrels at 7.3 bbl/t (API standard).

#### Key Dependencies

- `dbscan`, `mclust`, `data.table`, `geosphere`.
- AISStream.io API key.
- Keep-Awake Utility. Use a keep-awake utility such as Caffeinate, Amphetamine, or a similar tool to prevent your system from sleeping during operation.

#### Output

Each confirmed event record includes vessel identifiers, event type (loading/unloading), port, timing, vessel class, and mass estimates in tonnes and barrels. Events are appended to `data/output/all_cargo_events.rds`.

**Vessel classes detected:** Small Tanker, Handysize, Handymax, Panamax, Aframax, Suezmax, VLCC, ULCC.

#### Known Limitations

- DWT regression carries uncertainty; planned upgrade to cluster-stratified or tree-based model.
- Single terrestrial AIS feed causes offshore coverage gaps.
- 61 hardcoded port bounding boxes; unmatched locations labelled *Lost at Sea*.
- Floating storage events not yet classified.
- Vessel class lookup table could be better utilized with length and breadth.
- DBSCAN minimum points should increase. If static data for a specific MMSI is streamed every 6 minutes, then 2 points implies that a tanker only spend 12 minutes at a specific draught. This is impractical for movement, or for loading/unloading. Additionally, it would capture the changes in draught as it occurs instead of the entire change in draught, giving multiple events for each event. Given that unloading/loading events can be as short as 8 hours and trips are almost always longer than 8 hours (and for our interests always are), I would suggest to use minimum points = 7 hours * 10 points/hour = 70, which provides leeway on the short end. 
- The port-lookup table needs to be replaced. It is from a previous version that no longer requires all of its uses, so now it overly complicates the tables creation. 

#### References
<!--
Data

AISStream.io. Real-time AIS WebSocket API. https://aisstream.io/
Danish Maritime Authority. AIS data download. https://www.dma.dk/safety-at-sea/navigational-information/download-data
NOAA Office for Coastal Management. (2025). AIS data handler. https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/index.html
NOAA Office for Coastal Management. (2018). Vessel type codes [Data file]. https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf
Warrant Group. IMO vessel codes [Data set]. GitHub. https://github.com/warrantgroup/IMO-Vessel-Codes/blob/master/data/imo-vessel-codes.csv
HI Nelson. (2019). Seaports of the world [PDF]. https://www.hinelson.com/blog/wp-content/uploads/2019/09/Seaports-of-the-World.pdf
Equasis. Ship information search. https://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search

Methodology

Adland, R., Jia, H., & Strandenes, S. P. (2017). Are AIS-based trade volume estimates reliable? The case of crude oil exports. Maritime Policy & Management, 44(5), 657–665. https://doi.org/10.1080/03088839.2017.1309470
Jia, H., Prakash, V., & Smith, T. (2019). Estimating vessel payloads in bulk shipping using AIS data. International Journal of Shipping and Transport Logistics, 11(1), 25–40. https://doi.org/10.1504/IJSTL.2019.096864
Kalokairinos, E., Mavroeidis, T., Radou, G., & Zachariou, Z. (2000–2005). Regression analysis of basic ship design values for merchant ships [Diploma theses]. National Technical University of Athens.
Papanikolaou, A. (2014). Ship design: Methodologies of preliminary design. Springer. https://doi.org/10.1007/978-94-017-8751-2
Schneekluth, H., & Bertram, V. (1998). Ship design for efficiency and economy (2nd ed.). Butterworth-Heinemann.
Zhang, R., Dong, D., Chen, X., Zhang, B., Zhang, Y., Ye, L., Liu, B., Zhao, Y., & Peng, C. (2025). AIS data-driven analysis for identifying cargo handling events in international trade tankers. Ocean Engineering, 317, Article 120016. https://doi.org/10.1016/j.oceaneng.2024.120016

Other

Enercón Group. Tanker definitions. https://www.enercongroup.com/definitionstanker.html
Raymond, E. S. AIVDM/AIVDO protocol decoding: Type 8 binary broadcast message. https://gpsd.gitlab.io/gpsd/AIVDM.html#_type_8_binary_broadcast_message
International Maritime Organization. (2015). Revised guidelines for the onboard operational use of shipborne automatic identification systems (AIS) (Resolution A.1106(29)). https://wwwcdn.imo.org/localresources/en/OurWork/Safety/Documents/IMO%20Documents%20related%20to/Resolution%20A.1106(29).pdf
International Maritime Organization. (2012). Amendments to the international code for the construction and equipment of ships carrying dangerous chemicals in bulk (IBC Code) (Resolution MSC.340(91)). https://wwwcdn.imo.org/localresources/en/KnowledgeCentre/IndexofIMOResolutions/MSCResolutions/MSC.340(91).pdf
MARPOL Training Institute. MARPOL 73/78, Annex I, Regulation 18: Segregated ballast tanks. https://www.marpoltraininginstitute.com/MMSKOREAN/MARPOL/Annex_I/r18.htm
Port Economics, Management and Policy. Tanker size categories. https://porteconomicsmanagement.org/pemp/contents/part5/ports-and-energy/tanker-size/
U.S. Energy Information Administration. (2014). Tanker sizes and classes. https://www.eia.gov/todayinenergy/detail.php?id=17991
-->

**Data**

- AISStream.io. *Real-time AIS WebSocket API*. https://aisstream.io/.
- Danish Maritime Authority. *AIS data download*. https://www.dma.dk/safety-at-sea/navigational-information/download-data.
- NOAA Office for Coastal Management. (2025). *AIS data handler*. https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/index.html.
- NOAA Office for Coastal Management. (2018). *Vessel type codes*. https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf.
- Warrant Group. *IMO vessel codes* [Data set]. GitHub. https://github.com/warrantgroup/IMO-Vessel-Codes/blob/master/data/imo-vessel-codes.csv.
- HI Nelson. (2019). *Seaports of the world*. https://www.hinelson.com/blog/wp-content/uploads/2019/09/Seaports-of-the-World.pdf.
- Equasis. *Ship information search*. https://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search.

**Methodology**

- Adland, R., Jia, H., & Strandenes, S. P. (2017). Are AIS-based trade volume estimates reliable? The case of crude oil exports. *Maritime Policy & Management*, *44*(5), 657–665. https://doi.org/10.1080/03088839.2017.1309470.
- Jia, H., Prakash, V., & Smith, T. (2019). Estimating vessel payloads in bulk shipping using AIS data. *International Journal of Shipping and Transport Logistics*, *11*(1), 25–40. https://doi.org/10.1504/IJSTL.2019.096864.
- Kalokairinos, E., Mavroeidis, T., Radou, G., & Zachariou, Z. (2000–2005). *Regression analysis of basic ship design values for merchant ships* [Diploma theses]. National Technical University of Athens.
- Papanikolaou, A. (2014). *Ship design: Methodologies of preliminary design*. Springer Dordrecht. https://doi.org/10.1007/978-94-017-8751-2.
- Schneekluth, H., & Bertram, V. (1998). *Ship design for efficiency and economy* (2nd ed.). Butterworth-Heinemann.
- Zhang, R., Dong, D., Chen, X., Zhang, B., Zhang, Y., Ye, L., Liu, B., Zhao, Y., & Peng, C. (2025). AIS data-driven analysis for identifying cargo handling events in international trade tankers. *Ocean Engineering*, *317*, 120016. https://doi.org/10.1016/j.oceaneng.2024.120016.

**Other**

- Enercón Group. *Tanker definitions*. https://www.enercongroup.com/definitionstanker.html.
- Eric S. Raymond. *AIVDM/AIVDO protocol decoding: Type 8 binary broadcast message*. https://gpsd.gitlab.io/gpsd/AIVDM.html#_type_8_binary_broadcast_message.
- International Maritime Organization. (2015). *Revised guidelines for the onboard operational use of shipborne automatic identification systems (AIS)* (Resolution A.1106(29)). https://wwwcdn.imo.org/localresources/en/OurWork/Safety/Documents/IMO%20Documents%20related%20to/Resolution%20A.1106(29).pdf.
- International Maritime Organization. (2012). *Amendments to the international code for the construction and equipment of ships carrying dangerous chemicals in bulk (IBC Code)* (Resolution MSC.340(91)). https://wwwcdn.imo.org/localresources/en/KnowledgeCentre/IndexofIMOResolutions/MSCResolutions/MSC.340(91).pdf.
- MARPOL Training Institute. *MARPOL 73/78, Annex I, Regulation 18: Segregated ballast tanks*. https://www.marpoltraininginstitute.com/MMSKOREAN/MARPOL/Annex_I/r18.htm.
- Port Economics, Management and Policy. *Tanker size categories*. https://porteconomicsmanagement.org/pemp/contents/part5/ports-and-energy/tanker-size/.
- U.S. Energy Information Administration. (2014). *Tanker sizes and classes*. https://www.eia.gov/todayinenergy/detail.php?id=17991.

#### Note

- This project is a work in progress. All cargo estimates — including payload mass, draught changes, and barrel conversions — are approximations derived from AIS data and author-defined models; they are not guaranteed to be accurate and should not be treated as such.
- Due to the concurrent, iterative nature of the four-process pipeline, processes can execute indefinitely (this is my intent). To be diligent before running this program, ensure you are familiar with your system's process management tools (Activity Monitor on macOS, Task Manager on Windows) so you can manually terminate processes if required.

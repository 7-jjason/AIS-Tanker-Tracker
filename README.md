## Maritime Tanker Tracker

A real-time AIS data pipeline for detecting cargo-handling events and estimating vessel payload, implemented in R across four concurrent processes.

#### Overview

This program ingests live global AIS data, isolates tanker vessels, cleans data, detects cargo loading events, estimates the cargo mass loaded/unloaded in metric tonnes and barrels (x1000) by port and nation.

#### Pipeline

| Process | Role |
|---|---|
| **P1 – Ingestion** | Connects to AISStream.io via WebSocket; buffers raw hex messages to disk. |
| **P2 – Parser** | Filters at hex level; parses tanker messages; DBSCAN; writes RDS files. |
| **P3 – Analysis** | Re-filter; draught-change detection; GMM berthing confirmation; payload estimation (24h cycle). |
| **P4 – Storage** | Writes P3 output to MySQL (not included here). |
| **Background Launcher** | Runs each process, deals with WiFi connectivity, emergency shutdowns, and hung processes. | 

#### Installation

(1) Download:
  - files/process_1_weksocket_receiver.R
  - files/process_2_parser_filter.R
  - files/process_3_Zhang_et_al.R
  - files/background_launcher.R
  - files/setup.R
  - files/kill_process.R
  - data/tanker_mmsi_registry.rds
(2) Add API key from AISStream.io as a .R file in /files.
(3) Execute setup.R and follow any instructions.
(4) To begin the program, execute background_launcher.R.
(5a) To stop the program, execute kill_process.R.
(5b) If process does not stop using kill_process.R, use Activity Monitor on macOS or Task Manager on Windows.

#### Key Dependencies

- Keep-Awake Utility such as Caffeinate, Amphetamine, or a similar tool to prevent your system from sleeping during operation.

#### Output

- Each confirmed event record includes vessel identifiers, event type (loading/unloading), port, timing, vessel class, and mass estimates in tonnes and barrels. Events are appended to `data/output/all_cargo_events.rds`.

#### Known Limitations

- DWT calculation does not account for changes in ballast that occur while loading/unloading. 
- Single terrestrial AIS feed causes offshore coverage gaps.

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
- Equasis. *Ship information search*. https://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search.
- HI Nelson. (2019). *Seaports of the world*. https://www.hinelson.com/blog/wp-content/uploads/2019/09/Seaports-of-the-World.pdf.
- NOAA Office for Coastal Management. (2025). *AIS data handler*. https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025/index.html.
- NOAA Office for Coastal Management. (2018). *Vessel type codes*. https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf.
- Warrant Group. *IMO vessel codes* [Data set]. GitHub. https://github.com/warrantgroup/IMO-Vessel-Codes/blob/master/data/imo-vessel-codes.csv.

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

- All cargo estimates — including payload mass, draught changes, and barrel conversions — are approximations; they are not guaranteed to be accurate and should not be treated as such.
- Due to the concurrent, iterative nature of the four-process pipeline, processes can intentionally execute indefinitely. To be diligent before running this program, ensure you are familiar with your system's process management tools (Activity Monitor on macOS, Task Manager on Windows) to manually terminate processes if required.

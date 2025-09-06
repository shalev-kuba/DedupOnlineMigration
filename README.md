# DedupOnlineMigration

## Paper
This project implements the online deduplicated migration algorithms presented in the paper:  
*“Let It Slide: Online Deduplicated Data Migration.”* SYSTOR ’25: Proceedings of the 18th ACM International Systems and Storage Conference  
[https://dl.acm.org/doi/10.1145/3757347.3759144](https://dl.acm.org/doi/10.1145/3757347.3759144)

- Authors: Shalev Kuba and Gala Yadgar, Technion – Israel Institute of Technology
- Email: s.kuba@campus.technion.ac.il

---

## Project Layout
- **SystemsAndChangesCreation**: Contains all code and scripts used to create the different  
  workloads and change lists used in the paper.

- **OnlineAlgorithms**: Contains implementations of the online deduplication algorithms,  
  based on two offline deduplication algorithms: (1) HC and (2) Greedy. Both are described in  
  *“The What, The From, and The To: The Migration Games in Deduplicated Systems.”*  
  20th USENIX Conference on File and Storage Technologies (FAST ’22), 2022  
  <https://www.usenix.org/conference/fast22/presentation/kisous>  
  <https://github.com/roei217/DedupMigration/>

- **Experiments**: Contains parameters documentation and scripts for generating the graphs  
  presented in the paper.  

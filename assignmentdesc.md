Implementation of Ricart-Argawala

Description:

You are required to implement distributed mutual exclusion among nodes
in a distributed system. Your implementation must use the
Ricart-Argawala algorithm discussed during the lectures.

System Requirements:

R1 (Spec): Implement a system with a set of nodes and a Critical
Section that represents a sensitive system operation. Any node may
request access to the Critical Section at any time. In this exercise,
the Critical Section can be emulated, for example, by a print statement or
writing to a shared database on the network

R2 (Safety): Only one node may enter the Critical Section at any time

R3 (Liveliness): Every node that requests access to the Critical
Section will eventually gain access


Technical Requirements:

- Implement the service's nodes using Golang

- Provide a README.md file in your source code repository that
  explains how to start your system

- Use gRPC for message passing between nodes (hint: each node is both
  a grpc server and a client to every other node)

- Demonstrate that the system can start with at least three nodes

- Show, using system logs, a sequence of messages that leads to a node
  gaining access to the Critical Section.


Note. Implement some sort of service discovery so that nodes can locate each
other. Options to consider include:

- hardcoding in each node (easy)
- Supplying a file with IP addresses and ports of other nodes
- Entering IP addresses and ports via the command line
- Using an existing package or service for discovery


Hand-in Requirements:

- Submit a single report as a PDF file (maximum 2 pages)

- Describe how your system meets the System Requirements (R1, R2, and R3)

- Provide a discussion of your algorithm, using examples from your
  logs (as per Technical Requirement 5)

- Include a link to a Git repository with your source code in the report

- Append system logs in the report's appendix to demonstrate that the
  requirements are met
Earth satellite observation is known for more than fifty years, which has resulted the accumulation of huge amount of digital data. This huge amount of data makes the process of storing and processing difficult and time consuming. For this reason, the ideal way to store and process the data is in distributed systems, using parallel and distributed programming.
In this thesis, is implemented an application that processes satellite images for burn scar mapping, using the programming model MapReduce and the Hadoop framework, which is designed to process large amounts of data in a distributed environment and it is open source.
The implementation of the application based on the Burn Scar Mapping algorithm, developed at the National Observatory of Athens. The application is divided into five processing stages: converting satellite images in a format suitable for processing by the mappers, classification of the “burned” pixels of the image, application of a 3x3 median filter, noise reduction using "connect components" algorithm, generation of the final output image. The three intermediate stages are implemented on the Hadoop, the other two in Java.
The application ran into the modern cloud environment of Microsoft, and the produced results were satisfactory, cause of parallel processing of eight satellite images and exporting eight mapped images.
"""
Example integrations with popular ML frameworks for using the HighPerformanceS3Downloader
in data pipelines or training workflows.
"""

import os
import time
import torch
from torch.utils.data import Dataset, DataLoader
import tensorflow as tf
from typing import List, Dict, Any, Callable, Optional, Tuple

# Import the high-performance downloader
from high_performance_s3_downloader import HighPerformanceS3Downloader


class S3CachedDataset(Dataset):
    """
    PyTorch Dataset that efficiently downloads data from S3 and caches it locally.
    Designed to be used with the HighPerformanceS3Downloader for maximum performance.
    """

    def __init__(
        self,
        bucket: str,
        keys: List[str],
        cache_dir: str,
        transform: Optional[Callable] = None,
        s3_config: Dict[str, Any] = None,
        download_batch_size: int = 1000,
        processes: int = None,
        concurrency_per_process: int = 25,
        prefetch: bool = True,
        delete_on_exit: bool = False
    ):
        """
        Initialize the dataset with S3 information and performance parameters.

        Args:
            bucket: S3 bucket name
            keys: List of S3 object keys to use in the dataset
            cache_dir: Local directory to cache downloaded files
            transform: PyTorch transform to apply to loaded data
            s3_config: Dictionary with S3 configuration (endpoint_url, aws_access_key_id, etc.)
            download_batch_size: Number of files to download in each batch
            processes: Number of processes for the downloader (defaults to CPU count)
            concurrency_per_process: Concurrent downloads per process
            prefetch: Whether to start downloading files immediately upon initialization
            delete_on_exit: Whether to delete cached files when the dataset is destroyed
        """
        self.bucket = bucket
        self.keys = keys
        self.cache_dir = cache_dir
        self.transform = transform
        self.s3_config = s3_config or {}
        self.download_batch_size = download_batch_size
        self.processes = processes
        self.concurrency_per_process = concurrency_per_process
        self.prefetch = prefetch
        self.delete_on_exit = delete_on_exit

        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

        # Initialize the downloader
        self.downloader = HighPerformanceS3Downloader(
            endpoint_url=self.s3_config.get('endpoint_url'),
            aws_access_key_id=self.s3_config.get('aws_access_key_id'),
            aws_secret_access_key=self.s3_config.get('aws_secret_access_key'),
            region_name=self.s3_config.get('region_name'),
            processes=self.processes,
            concurrency_per_process=self.concurrency_per_process
        )

        # Track downloaded files
        self.downloaded_files = set()

        # Start prefetching if requested
        if self.prefetch:
            self._start_prefetch()

    def _start_prefetch(self):
        """Start prefetching files in a separate thread"""
        import threading

        def prefetch_worker():
            for i in range(0, len(self.keys), self.download_batch_size):
                batch_keys = self.keys[i:i+self.download_batch_size]
                self._ensure_files_available(batch_keys)

        thread = threading.Thread(target=prefetch_worker)
        thread.daemon = True
        thread.start()

    def _ensure_files_available(self, keys: List[str]) -> None:
        """Ensure that the given keys are available locally, downloading if necessary"""
        # Check which files need to be downloaded
        to_download = []
        for key in keys:
            local_path = os.path.join(self.cache_dir, os.path.basename(key))
            if not os.path.exists(local_path) and key not in self.downloaded_files:
                to_download.append(key)

        # Download files if needed
        if to_download:
            print(f"Downloading {len(to_download)} files...")
            stats = self.downloader.download_files(
                bucket=self.bucket,
                keys=to_download,
                destination_dir=self.cache_dir
            )
            # Mark files as downloaded
            self.downloaded_files.update(to_download)

    def _get_local_path(self, key: str) -> str:
        """Get the local path for a key"""
        return os.path.join(self.cache_dir, os.path.basename(key))

    def __len__(self) -> int:
        """Return the total number of samples in the dataset"""
        return len(self.keys)

    def __getitem__(self, idx: int) -> Any:
        """Get a sample from the dataset"""
        key = self.keys[idx]
        local_path = self._get_local_path(key)

        # Ensure the file is available locally
        if not os.path.exists(local_path):
            self._ensure_files_available([key])

        # Load the file (implementation depends on file type)
        data = self._load_file(local_path)

        # Apply transform if provided
        if self.transform:
            data = self.transform(data)

        return data

    def _load_file(self, path: str) -> Any:
        """
        Load a file from the local path.

        This method should be overridden by subclasses to handle specific file types.
        """
        raise NotImplementedError("Subclasses must implement _load_file")

    def __del__(self):
        """Cleanup on dataset destruction"""
        if self.delete_on_exit and os.path.exists(self.cache_dir):
            import shutil
            shutil.rmtree(self.cache_dir)


class S3ImageDataset(S3CachedDataset):
    """Dataset for loading image data from S3"""

    def _load_file(self, path: str) -> torch.Tensor:
        """Load an image file"""
        from PIL import Image
        import numpy as np

        # Load image using PIL
        with Image.open(path) as img:
            img = img.convert('RGB')
            # Convert to tensor (typically done in transform, but included here for completeness)
            np_img = np.array(img)
            tensor = torch.from_numpy(np_img).permute(2, 0, 1).float() / 255.0

        return tensor


class S3TFDataset:
    """
    TensorFlow dataset that efficiently downloads data from S3 and caches it locally.
    """

    def __init__(
        self,
        bucket: str,
        keys: List[str],
        cache_dir: str,
        parse_fn: Callable,
        s3_config: Dict[str, Any] = None,
        download_batch_size: int = 1000,
        processes: int = None,
        concurrency_per_process: int = 25,
        prefetch: bool = True,
        delete_on_exit: bool = False
    ):
        """
        Initialize the dataset with S3 information and performance parameters.

        Args:
            bucket: S3 bucket name
            keys: List of S3 object keys to use in the dataset
            cache_dir: Local directory to cache downloaded files
            parse_fn: Function to parse a file path into TensorFlow tensor(s)
            s3_config: Dictionary with S3 configuration (endpoint_url, aws_access_key_id, etc.)
            download_batch_size: Number of files to download in each batch
            processes: Number of processes for the downloader (defaults to CPU count)
            concurrency_per_process: Concurrent downloads per process
            prefetch: Whether to start downloading files immediately upon initialization
            delete_on_exit: Whether to delete cached files when the dataset is destroyed
        """
        self.bucket = bucket
        self.keys = keys
        self.cache_dir = cache_dir
        self.parse_fn = parse_fn
        self.s3_config = s3_config or {}
        self.download_batch_size = download_batch_size
        self.processes = processes
        self.concurrency_per_process = concurrency_per_process
        self.prefetch = prefetch
        self.delete_on_exit = delete_on_exit

        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

        # Initialize the downloader
        self.downloader = HighPerformanceS3Downloader(
            endpoint_url=self.s3_config.get('endpoint_url'),
            aws_access_key_id=self.s3_config.get('aws_access_key_id'),
            aws_secret_access_key=self.s3_config.get('aws_secret_access_key'),
            region_name=self.s3_config.get('region_name'),
            processes=self.processes,
            concurrency_per_process=self.concurrency_per_process
        )

        # Track downloaded files
        self.downloaded_files = set()

        # Start prefetching if requested
        if self.prefetch:
            self._start_prefetch()

    def _start_prefetch(self):
        """Start prefetching files in a separate thread"""
        import threading

        def prefetch_worker():
            for i in range(0, len(self.keys), self.download_batch_size):
                batch_keys = self.keys[i:i+self.download_batch_size]
                self._ensure_files_available(batch_keys)

        thread = threading.Thread(target=prefetch_worker)
        thread.daemon = True
        thread.start()

    def _ensure_files_available(self, keys: List[str]) -> None:
        """Ensure that the given keys are available locally, downloading if necessary"""
        # Check which files need to be downloaded
        to_download = []
        for key in keys:
            local_path = os.path.join(self.cache_dir, os.path.basename(key))
            if not os.path.exists(local_path) and key not in self.downloaded_files:
                to_download.append(key)

        # Download files if needed
        if to_download:
            print(f"Downloading {len(to_download)} files...")
            stats = self.downloader.download_files(
                bucket=self.bucket,
                keys=to_download,
                destination_dir=self.cache_dir
            )
            # Mark files as downloaded
            self.downloaded_files.update(to_download)

    def _get_local_path(self, key: str) -> str:
        """Get the local path for a key"""
        return os.path.join(self.cache_dir, os.path.basename(key))

    def get_dataset(self) -> tf.data.Dataset:
        """Convert to a TensorFlow Dataset"""
        # Ensure all files are available if prefetch is disabled
        if not self.prefetch:
            self._ensure_files_available(self.keys)

        # Get local paths for all keys
        local_paths = [self._get_local_path(key) for key in self.keys]

        # Create TensorFlow dataset
        ds = tf.data.Dataset.from_tensor_slices(local_paths)
        ds = ds.map(self.parse_fn, num_parallel_calls=tf.data.AUTOTUNE)

        return ds

    def __del__(self):
        """Cleanup on dataset destruction"""
        if self.delete_on_exit and os.path.exists(self.cache_dir):
            import shutil
            shutil.rmtree(self.cache_dir)


# Example usage for PyTorch
def example_pytorch_usage():
    """Example of using the S3CachedDataset with PyTorch DataLoader"""

    from torchvision import transforms

    # S3 configuration
    s3_config = {
        'endpoint_url': 'https://s3.example.com',
        'aws_access_key_id': 'your_access_key',
        'aws_secret_access_key': 'your_secret_key',
        'region_name': 'us-east-1'
    }

    # Create dataset
    dataset = S3ImageDataset(
        bucket='my-bucket',
        keys=['images/img1.jpg', 'images/img2.jpg', ...],  # List of image keys
        cache_dir='/tmp/dataset_cache',
        transform=transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ]),
        s3_config=s3_config,
        processes=4,  # Use 4 processes for downloading
        concurrency_per_process=25  # 25 concurrent downloads per process
    )

    # Create data loader
    dataloader = DataLoader(
        dataset,
        batch_size=32,
        shuffle=True,
        num_workers=4,  # DataLoader workers for parallel loading
        pin_memory=True  # Faster data transfer to GPU
    )

    # Training loop
    for epoch in range(10):
        for batch in dataloader:
            # Process batch
            pass


# Example usage for TensorFlow
def example_tensorflow_usage():
    """Example of using the S3TFDataset with TensorFlow"""

    # Define parse function for TensorFlow
    def parse_image(filepath):
        """Parse an image file into a tensor"""
        img = tf.io.read_file(filepath)
        img = tf.image.decode_jpeg(img, channels=3)
        img = tf.image.resize(img, [224, 224])
        img = tf.cast(img, tf.float32) / 255.0
        return img

    # S3 configuration
    s3_config = {
        'endpoint_url': 'https://s3.example.com',
        'aws_access_key_id': 'your_access_key',
        'aws_secret_access_key': 'your_secret_key',
        'region_name': 'us-east-1'
    }

    # Create dataset
    s3_dataset = S3TFDataset(
        bucket='my-bucket',
        keys=['images/img1.jpg', 'images/img2.jpg', ...],  # List of image keys
        cache_dir='/tmp/dataset_cache',
        parse_fn=parse_image,
        s3_config=s3_config,
        processes=4,  # Use 4 processes for downloading
        concurrency_per_process=25  # 25 concurrent downloads per process
    )

    # Get TensorFlow dataset
    dataset = s3_dataset.get_dataset()

    # Apply standard TensorFlow data pipeline operations
    dataset = dataset.batch(32).prefetch(tf.data.AUTOTUNE)

    # Training loop
    for epoch in range(10):
        for batch in dataset:
            # Process batch
            pass


# Example implementation for HuggingFace datasets
def example_huggingface_usage():
    """Example of integrating with HuggingFace datasets"""
    from datasets import Dataset, Features, Value, Image
    import pandas as pd

    # S3 configuration
    s3_config = {
        'endpoint_url': 'https://s3.example.com',
        'aws_access_key_id': 'your_access_key',
        'aws_secret_access_key': 'your_secret_key',
        'region_name': 'us-east-1'
    }

    # Initialize downloader
    downloader = HighPerformanceS3Downloader(
        endpoint_url=s3_config['endpoint_url'],
        aws_access_key_id=s3_config['aws_access_key_id'],
        aws_secret_access_key=s3_config['aws_secret_access_key'],
        region_name=s3_config['region_name'],
        processes=4,
        concurrency_per_process=25
    )

    # List of file keys in S3
    keys = ['images/img1.jpg', 'images/img2.jpg', ...]  # List of image keys

    # Download files
    cache_dir = '/tmp/hf_dataset_cache'
    downloader.download_files(
        bucket='my-bucket',
        keys=keys,
        destination_dir=cache_dir
    )

    # Create dataframe with local paths
    df = pd.DataFrame({
        'file_path': [os.path.join(cache_dir, os.path.basename(key)) for key in keys],
        'label': [0, 1, ...]  # Your labels here
    })

    # Create HuggingFace dataset
    features = Features({
        'file_path': Value('string'),
        'image': Image(),
        'label': Value('int32')
    })

    dataset = Dataset.from_pandas(df, features=features)

    # Use the dataset
    dataset = dataset.map(
        lambda example: {'image': example['file_path']},
        remove_columns=['file_path']
    )

    # Now dataset can be used with HuggingFace Transformers, etc.
    return dataset


# Example for creating a ray data dataset with S3 downloader
def example_ray_usage():
    """Example of integrating with Ray Data for distributed training"""
    import ray
    from ray.data import read_images

    # Initialize Ray (adjust resources as needed)
    ray.init()

    # S3 configuration
    s3_config = {
        'endpoint_url': 'https://s3.example.com',
        'aws_access_key_id': 'your_access_key',
        'aws_secret_access_key': 'your_secret_key',
        'region_name': 'us-east-1'
    }

    # Initialize downloader
    downloader = HighPerformanceS3Downloader(
        endpoint_url=s3_config['endpoint_url'],
        aws_access_key_id=s3_config['aws_access_key_id'],
        aws_secret_access_key=s3_config['aws_secret_access_key'],
        region_name=s3_config['region_name'],
        processes=4,
        concurrency_per_process=25
    )

    # List of file keys in S3
    keys = ['images/img1.jpg', 'images/img2.jpg', ...]  # List of image keys

    # Download files
    cache_dir = '/tmp/ray_dataset_cache'
    downloader.download_files(
        bucket='my-bucket',
        keys=keys,
        destination_dir=cache_dir
    )

    # Create Ray dataset from local files
    ds = read_images(cache_dir)

    # Apply transformations
    ds = ds.map_batches(
        lambda batch: {"image": batch["image"] / 255.0},
        batch_format="pandas"
    )

    # Use with Ray Train or other Ray libraries
    return ds

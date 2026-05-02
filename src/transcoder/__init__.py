"""
Dropbox Video Transcoder - H.264 to H.265/HEVC transcoding daemon.

A robust, idempotent daemon that monitors a Dropbox folder, downloads videos,
transcodes H.264 to H.265 (HEVC) using FFmpeg with hardware acceleration support,
and uploads the result back to Dropbox.
"""

__version__ = "6.3.2"
__author__ = "Dropbox Video Transcoder Team"

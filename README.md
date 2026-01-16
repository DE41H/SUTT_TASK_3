# 📘 Campus Forum & Discussion Platform

![License](https://img.shields.io/badge/license-MIT-blue.svg) ![Python](https://img.shields.io/badge/python-3.10%2B-blue) ![Django](https://img.shields.io/badge/django-5.0-green) ![Status](https://img.shields.io/badge/status-active-success)

A robust, full-stack discussion platform tailored for academic environments. Built with **Django**, this application facilitates focused discussions, resource sharing, and community moderation, featuring real-time email notifications, advanced search, and a modern, responsive UI.

---

## 🚀 Key Features

### 💬 Discussion & Community
*   **Threaded Conversations:** Create rich-text discussions with Markdown support.
*   **Nested Replies:** Deeply integrated reply system to keep conversations organized.
*   **Smart Tagging:** Organize threads with custom hex-colored tags, plus integration with **Courses** and **Documents**.
*   **Upvote System:** Reddit-style upvoting for threads and replies to highlight quality content.
*   **Mentions (@user):** Tag users in posts to notify them instantly via email.

### 🔍 Search & Discovery
*   **Trigram Similarity Search:** Custom-built fuzzy search engine that finds results even with typos (e.g., "engnering" finds "Engineering").
*   **Advanced Filtering:** Sort by "Newest" or "Top Rated" and filter by multiple tags simultaneously.
*   **Category Organization:** Clean URL slugs for intuitive navigation (e.g., `/categories/computer-science/`).

### 🛡️ Moderation & Safety
*   **User Reporting System:** Users can report threads or replies. Admins view a dedicated **Moderation Queue**.
*   **Automatic Content Sanitization:** Uses `bleach` to prevent XSS attacks while allowing safe HTML.
*   **Rate Limiting:** Prevents spam by limiting how fast users can post or report content.
*   **Soft Deletion:** "Deleted" content is hidden from view but preserved in the database for audit trails.
*   **Lock Threads:** Moderators can freeze discussions to prevent further replies.

### ⚡ Performance & Engineering
*   **Asynchronous Emails:** Threaded email dispatcher ensures the UI never freezes when sending notifications.
*   **Database Atomic Transactions:** Ensures data integrity during complex operations (e.g., upvoting while updating counts).
*   **Efficient Bulk Operations:** Optimized `bulk_create` for tags and trigrams to minimize database hits.

---

## 🛠️ Architecture & Database Design

The database schema is designed for **scalability** and **data integrity**.

### Core Models

| Model | Role & Design Choice |
| :--- | :--- |
| **User** | Leveraging Django's `AUTH_USER_MODEL` for flexibility. |
| **Category** | Acts as the primary container for threads. Uses **unique slugs** for SEO-friendly URLs. |
| **Thread** | The central entity. Inherits from an abstract `Post` class to share logic (upvotes, content sanitization) with Replies. |
| **Reply** | Linked to Threads via Foreign Key. Includes a `reply_count` denormalization on the Thread model for rapid listing performance. |
| **Tag** | Many-to-Many relationship with Threads. Stores `hex_color` for visual distinction. |
| **Report** | Polymorphic-style design: Can link to *either* a Thread or a Reply. Includes a status workflow (`PENDING` -> `RESOLVED`). |

### 🧠 The "Fuzzy Search" Engine (Trigrams)

Instead of a heavy search engine like Elasticsearch, we implemented a lightweight, pure-database search solution:

1.  **Trigram Model:** Stores 3-character slices of thread titles (e.g., "Python" -> `pyt`, `yth`, `tho`, `hon`).
2.  **Reverse Indexing:** When a user searches, the query is sliced into trigrams.
3.  **Scoring:** Threads are ranked by how many matching trigrams they share with the query.
4.  **Why?** This allows for highly effective "fuzzy" matching (handling typos) without needing external dependencies.

---

## 📸 Feature Walkthrough

### 1. Creating a Thread
Rich text editing with Markdown support. Users can tag specific **Courses** (e.g., `CS101`) or **Documents** to link discussions to study materials.

### 2. The Moderation Queue
Admins have a dashboard to review reports.
*   **Yellow Badge:** Pending review.
*   **Green Badge:** Resolved.
*   **Actions:** One-click "Delete Content" or "Mark Resolved".

### 3. Email Notifications
The system intelligently notifies users when:
*   Someone replies to their thread.
*   They are mentioned (`@username`) in a post.
*   *Note: Emails are sent in background threads to maintain UI responsiveness.*

---

## 📦 Tech Stack

*   **Backend:** Django 5.0, Python 3.11+
*   **Database:** PostgreSQL
*   **Frontend:** Django Templates, Bootstrap 5, Bootstrap Icons

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:
1.  Fork the repository.
2.  Create a feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.

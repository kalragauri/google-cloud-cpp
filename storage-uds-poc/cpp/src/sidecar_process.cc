// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sidecar_process.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <fcntl.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace gcs_core::internal {
namespace {

using ::google::cloud::Status;
using ::google::cloud::StatusCode;
using ::google::cloud::StatusOr;

/// The child sees the lifetime pipe at this descriptor.
constexpr int kChildLifetimeFd = 3;

bool IsExecutableFile(std::string const& path) {
  struct stat st{};
  return ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode) &&
         ::access(path.c_str(), X_OK) == 0;
}

Status Errno(std::string_view what) {
  return Status(StatusCode::kInternal,
                absl::StrCat(what, ": ", std::strerror(errno)));
}

std::string ExecutableDirectory() {
  std::string buf(4096, '\0');
  ssize_t const n = ::readlink("/proc/self/exe", buf.data(), buf.size() - 1);
  if (n <= 0) return {};
  buf.resize(static_cast<std::size_t>(n));
  std::size_t const slash = buf.rfind('/');
  return slash == std::string::npos ? std::string{} : buf.substr(0, slash);
}

}  // namespace

StatusOr<std::string> ResolveSidecarPath(std::string const& configured) {
  auto explicit_path = [](std::string const& path,
                          std::string_view source) -> StatusOr<std::string> {
    if (IsExecutableFile(path)) return path;
    return Status(StatusCode::kFailedPrecondition,
                  absl::StrCat("sidecar binary from ", source, " (", path,
                               ") is not an executable file"));
  };
  if (!configured.empty()) return explicit_path(configured, "ClientOptions");
  std::string const env_var(kSidecarEnvVar);
  if (char const* env = std::getenv(env_var.c_str());
      env != nullptr && *env != '\0') {
    return explicit_path(env, kSidecarEnvVar);
  }
  std::string const dir = ExecutableDirectory();
  if (!dir.empty()) {
    std::string sibling = absl::StrCat(dir, "/", kSidecarBinaryName);
    if (IsExecutableFile(sibling)) return sibling;
  }
  if (char const* path = std::getenv("PATH"); path != nullptr) {
    for (absl::string_view entry :
         absl::StrSplit(path, ':', absl::SkipEmpty())) {
      std::string candidate = absl::StrCat(entry, "/", kSidecarBinaryName);
      if (IsExecutableFile(candidate)) return candidate;
    }
  }
  return Status(
      StatusCode::kFailedPrecondition,
      absl::StrCat("cannot find ", kSidecarBinaryName, ": set ", kSidecarEnvVar,
                   ", place it next to the executable, or add it "
                   "to PATH"));
}

StatusOr<std::string> DefaultSocketPath() {
  pid_t const pid = ::getpid();
  if (char const* xdg = std::getenv("XDG_RUNTIME_DIR");
      xdg != nullptr && *xdg != '\0') {
    return absl::StrCat(xdg, "/gcs-core-", pid, ".sock");
  }
  std::string const dir = absl::StrCat("/tmp/gcs-core-", ::getuid());
  if (::mkdir(dir.c_str(), 0700) != 0 && errno != EEXIST) {
    return Errno(absl::StrCat("mkdir ", dir));
  }
  struct stat st{};
  if (::lstat(dir.c_str(), &st) != 0) return Errno(absl::StrCat("stat ", dir));
  // A pre-existing directory must be ours and private, otherwise another
  // user could replace the socket.
  if (!S_ISDIR(st.st_mode) || st.st_uid != ::getuid() ||
      (st.st_mode & 0077) != 0) {
    return Status(StatusCode::kFailedPrecondition,
                  absl::StrCat(dir, " is not a private directory owned by uid ",
                               ::getuid()));
  }
  return absl::StrCat(dir, "/", pid, ".sock");
}

Status ValidateSocketPath(std::string const& path) {
  if (path.empty()) {
    return Status(StatusCode::kInvalidArgument, "socket path is empty");
  }
  if (path.size() > kMaxSocketPathLength) {
    return Status(StatusCode::kInvalidArgument,
                  absl::StrCat("socket path is ", path.size(),
                               " bytes; the limit is ", kMaxSocketPathLength));
  }
  struct stat st{};
  if (::lstat(path.c_str(), &st) == 0 && !S_ISSOCK(st.st_mode)) {
    return Status(
        StatusCode::kFailedPrecondition,
        absl::StrCat("socket path ", path, " exists and is not a Unix socket"));
  }
  return {};
}

StatusOr<std::unique_ptr<SidecarProcess>> SidecarProcess::Spawn(
    std::string const& binary, std::string const& socket_path,
    std::vector<std::string> const& extra_args) {
  if (Status s = ValidateSocketPath(socket_path); !s.ok()) return s;
  int fds[2];
  if (::pipe2(fds, O_CLOEXEC) != 0) return Errno("pipe2");
  int read_fd = fds[0];
  int const write_fd = fds[1];
  // dup2(fd, fd) would keep O_CLOEXEC set, closing the pipe at exec.
  if (read_fd == kChildLifetimeFd) {
    read_fd = ::fcntl(fds[0], F_DUPFD_CLOEXEC, kChildLifetimeFd + 1);
    ::close(fds[0]);
    if (read_fd < 0) {
      ::close(write_fd);
      return Errno("fcntl");
    }
  }

  std::vector<std::string> args = {
      binary, absl::StrCat("--socket=", socket_path),
      absl::StrCat("--parent-fd=", kChildLifetimeFd)};
  args.insert(args.end(), extra_args.begin(), extra_args.end());
  std::vector<char*> argv;
  argv.reserve(args.size() + 1);
  for (std::string& a : args) argv.push_back(a.data());
  argv.push_back(nullptr);

  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init(&actions);
  posix_spawn_file_actions_adddup2(&actions, read_fd, kChildLifetimeFd);
  pid_t pid = 0;
  int const rc = ::posix_spawn(&pid, binary.c_str(), &actions, nullptr,
                               argv.data(), environ);
  posix_spawn_file_actions_destroy(&actions);
  ::close(read_fd);
  if (rc != 0) {
    ::close(write_fd);
    errno = rc;
    return Errno(absl::StrCat("posix_spawn ", binary));
  }
  return std::unique_ptr<SidecarProcess>(
      new SidecarProcess(pid, write_fd, socket_path));
}

SidecarProcess::SidecarProcess(pid_t pid, int lifetime_fd,
                               std::string socket_path)
    : pid_(pid),
      lifetime_fd_(lifetime_fd),
      socket_path_(std::move(socket_path)) {}

SidecarProcess::~SidecarProcess() { Terminate(); }

bool SidecarProcess::Exited() {
  if (wait_status_ || reaped_externally_) return true;
  int status = 0;
  pid_t const rc = ::waitpid(pid_, &status, WNOHANG);
  if (rc == pid_) {
    wait_status_ = status;
  } else if (rc < 0 && errno == ECHILD) {
    // Host set SIGCHLD=SIG_IGN/SA_NOCLDWAIT or reaped children via waitpid(-1).
    reaped_externally_ = true;
  }
  return wait_status_.has_value() || reaped_externally_;
}

std::string SidecarProcess::ExitDescription() const {
  if (reaped_externally_) return "reaped externally";
  if (!wait_status_) return "running";
  int const s = *wait_status_;
  if (WIFEXITED(s)) return absl::StrCat("exited with status ", WEXITSTATUS(s));
  if (WIFSIGNALED(s)) return absl::StrCat("killed by signal ", WTERMSIG(s));
  return absl::StrCat("wait status ", s);
}

bool SidecarProcess::WaitFor(std::chrono::milliseconds timeout) {
  std::chrono::steady_clock::time_point const deadline =
      std::chrono::steady_clock::now() + timeout;
  while (!Exited()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return true;
}

void SidecarProcess::Terminate() { Terminate(std::chrono::seconds(2)); }

void SidecarProcess::Terminate(std::chrono::milliseconds grace) {
  if (lifetime_fd_ >= 0) {
    ::close(lifetime_fd_);  // The sidecar sees EOF and shuts down gracefully.
    lifetime_fd_ = -1;
  }
  if (!WaitFor(grace)) {
    ::kill(pid_, SIGTERM);
    if (!WaitFor(std::chrono::seconds(2))) {
      ::kill(pid_, SIGKILL);
      WaitFor(std::chrono::seconds(5));
    }
  }
  // Remove only a Unix socket file (never a regular file or symlink).
  struct stat st{};
  if (::lstat(socket_path_.c_str(), &st) == 0 && S_ISSOCK(st.st_mode)) {
    ::unlink(socket_path_.c_str());
  }
}

}  // namespace gcs_core::internal

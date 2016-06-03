%global debug_package %{nil}
%define pkg_prefix %{?PACKAGE_PREFIX}%{!?PACKAGE_PREFIX:lemur}

Name: %{pkg_prefix}-hsm-agent
Version: %{?_gitver}%{!?_gitver:0.0.1}
Release: %{?dist}%{!?dist:1}

Vendor: Intel Corporation
Source: %{pkg_prefix}-%{version}.tar.gz
License: GPL
Summary: INSERT PRODUCT NAME HERE - Lustre HSM Agent
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires: golang >= 1.6
BuildRequires: pandoc
Requires: lustre >= %{?MIN_LUSTRE_VERSION}%{?!MIN_LUSTRE_VERSION:2.6.0}

%description
The Lustre HSM Agent provides a backend-agnostic HSM Agent for brokering
communications between a Lustre filesystem's HSM coordinator and
backend-specific data movers.

# FIXME: This stuff still links against liblustreapi.
%package -n %{pkg_prefix}-data-movers
Summary: INSERT PRODUCT NAME HERE - HSM Data Movers
License: Apache
Requires: %{pkg_prefix}-hsm-agent = %{version}

%description -n %{pkg_prefix}-data-movers
These data movers are designed to implement the Lustre HSM Agent's data
movement protocol. When associated with an HSM archive number, a data
mover fulfills data movement requests on behalf of the HSM Agent.

%package -n %{pkg_prefix}-testing
Summary: INSERT PRODUCT NAME HERE - Testing Collateral
License: Apache
Requires: %{pkg_prefix}-hsm-agent = %{version} %{pkg_prefix}-data-movers = %{version}

%description -n %{pkg_prefix}-testing
Contains testing collateral for the product. Not intended for production
installations.

# TODO: This has to be GPL because it links against liblustreapi. Can we
# fix that?
%package -n ldmc
Summary: INSERT PRODUCT NAME HERE - Data Movement Control
License: GPL
Requires: lustre >= %{?MIN_LUSTRE_VERSION}%{?!MIN_LUSTRE_VERSION:2.6.0}

%description -n ldmc
CLI for Lustre data movement control.

%prep
%setup -n %{pkg_prefix}-%{version}

%install
%{__make} install PREFIX=$RPM_BUILD_ROOT/%{_prefix}

%files
%defattr(-,root,root)
%{_bindir}/lhsmd
%{_mandir}/man1/lhsmd.1.gz

%files -n %{pkg_prefix}-data-movers
%defattr(-,root,root)
%{_bindir}/lhsm-plugin-posix
%{_bindir}/lhsm-plugin-s3

%files -n %{pkg_prefix}-testing
%defattr(-,root,root)
%{_bindir}/lhsm-plugin-noop

%files -n ldmc
%defattr(-,root,root)
%{_bindir}/ldmc
